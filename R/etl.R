#' Reproducible ETL for DB-IP "lite" datasets
#'
#' Orchestrates the full Extract-Transform-Load flow:
#' downloads a DB-IP "lite" dataset, reads it into a tidy tibble, optionally
#' persists it as `.rds` and/or into DuckDB.
#'
#' Special value `database = "asn_geo"` builds a unified IPv4 dataset by joining
#' ASN ranges to a geolocation dataset (country or city) using a point lookup
#' on the range start address. This is fast and reproducible, and good enough
#' for exploration; note that very large ASN ranges may span multiple countries.
#'
#' @param database One of `"country"`, `"city"`, `"asn"`, `"asn_geo"`.
#' @param version `"latest"` or `"YYYY-MM"`. For `"asn_geo"` and `"latest"`,
#'   the function resolves the latest available versions independently per
#'   source dataset.
#' @param geo For `database = "asn_geo"`, which geolocation dataset to use:
#'   `"country"` or `"city"`.
#' @param refresh If `TRUE`, re-download and re-build even if cached artifacts
#'   exist.
#' @param write_rds If `TRUE`, save the resulting tibble as `.rds` in `data_dir`.
#' @param rds_path Optional explicit `.rds` output path.
#' @param write_duckdb If `TRUE`, write the resulting tibble into DuckDB.
#' @param db_path DuckDB database file path.
#' @param table DuckDB table name (defaults to a versioned name).
#' @param cache_dir Directory for downloaded `.csv.gz` files.
#' @param data_dir Directory for transformed outputs.
#' @param base_url Base URL for DB-IP free downloads.
#'
#' @return A tibble with normalized columns including `ip_start`, `ip_end`,
#'   `ip_version`, `ip_start_int`, `ip_end_int` (IPv4 only).
#' @export
dbip_etl <- function(
    database = c("country", "city", "asn", "asn_geo"),
    version = "latest",
    geo = c("country", "city"),
    refresh = FALSE,
    write_rds = TRUE,
    rds_path = NULL,
    write_duckdb = FALSE,
    db_path = file.path(dbip_data_dir(), "dbip.duckdb"),
    table = NULL,
    cache_dir = dbip_cache_dir(),
    data_dir = dbip_data_dir(),
    base_url = "https://download.db-ip.com/free"
) {
  check_installed(c("dplyr", "tibble", "stringr"))
  database <- match.arg(database)
  geo <- match.arg(geo)

  if (!dir.exists(data_dir)) dir.create(data_dir, recursive = TRUE, showWarnings = FALSE)

  if (!is.character(version) || length(version) != 1) abort_pkg("`version` must be a single string.")

  if (identical(database, "asn_geo")) {
    df <- dbip_build_asn_geo(
      version = version,
      geo = geo,
      refresh = refresh,
      cache_dir = cache_dir,
      base_url = base_url
    )

    out_version <- attr(df, "dbip_version_label")
    if (is.null(out_version)) out_version <- version
    out_key <- paste0("asn_geo_", geo)
  } else {
    path <- dbip_download(
      database = database,
      version = version,
      dest_dir = cache_dir,
      overwrite = refresh,
      base_url = base_url
    )
    df <- dbip_read(path)
    df <- dbip_normalize_db_fields(df, database = database)
    out_version <- if (identical(version, "latest")) dbip_version_from_filename(path) else version
    out_key <- database
  }

  if (is.null(rds_path) && isTRUE(write_rds)) {
    rds_path <- file.path(data_dir, paste0("dbip-", out_key, "-lite-", out_version, ".rds"))
  }

  if (isTRUE(write_rds) && !is.null(rds_path)) {
    if (isTRUE(refresh) || !file.exists(rds_path)) {
      saveRDS(df, file = rds_path)
      # Sidecar metadata for reproducibility.
      writeLines(
        c(
          paste0("database=", database),
          paste0("geo=", if (identical(database, "asn_geo")) geo else ""),
          paste0("version=", out_version),
          paste0("versions=", if (!is.null(attr(df, "dbip_versions"))) paste(capture.output(str(attr(df, "dbip_versions"))), collapse = " ") else ""),
          paste0("built_at=", format(Sys.time(), "%Y-%m-%d %H:%M:%S %z"))
        ),
        con = paste0(rds_path, ".meta"),
        useBytes = TRUE
      )
    }
  }

  if (isTRUE(write_duckdb)) {
    if (is.null(table)) {
      table <- paste0("dbip_", out_key, "_", stringr::str_replace_all(out_version, "-", "_"))
    }
    dbip_write_duckdb(df = df, db_path = db_path, table = table, overwrite = TRUE)
  }

  df
}

#' @keywords internal
dbip_version_from_filename <- function(path) {
  bn <- basename(path)
  m <- stringr::str_match(bn, "-lite-(\\d{4}-\\d{2})\\.csv\\.gz$")
  if (!is.na(m[, 2])) return(m[, 2])
  NA_character_
}

#' @keywords internal
dbip_normalize_db_fields <- function(df, database) {
  database <- match.arg(database, c("country", "city", "asn"))

  nm <- names(df)
  # Country dataset often uses `country` or `country_code`.
  if ("country" %in% nm && !"country_code" %in% nm) {
    df <- dplyr::rename(df, country_code = country)
    nm <- names(df)
  }

  # ASN dataset often uses `as_number`/`name`.
  if (identical(database, "asn")) {
    if ("as_number" %in% nm && !"asn" %in% nm) {
      df <- dplyr::rename(df, asn = as_number)
      nm <- names(df)
    }
    if ("name" %in% nm && !"as_name" %in% nm) {
      df <- dplyr::rename(df, as_name = name)
      nm <- names(df)
    }
  }

  df
}

#' @keywords internal
dbip_build_asn_geo <- function(version, geo, refresh, cache_dir, base_url) {
  check_installed(c("duckdb", "DBI"))

  # Resolve versions independently when requested.
  if (identical(version, "latest")) {
    ver_asn <- dbip_latest_version(database = "asn", base_url = base_url)
    ver_geo <- dbip_latest_version(database = geo, base_url = base_url)
  } else {
    ver_asn <- version
    ver_geo <- version
  }

  p_asn <- dbip_download("asn", version = ver_asn, dest_dir = cache_dir, overwrite = refresh, base_url = base_url)
  p_geo <- dbip_download(geo, version = ver_geo, dest_dir = cache_dir, overwrite = refresh, base_url = base_url)

  asn_df <- dbip_normalize_db_fields(dbip_read(p_asn), database = "asn")
  geo_df <- dbip_normalize_db_fields(dbip_read(p_geo), database = geo)

  # Focus on IPv4 for the join (we have integer helpers for IPv4 only).
  asn_df <- dplyr::filter(asn_df, ip_version == 4L, !is.na(ip_start_int))
  geo_df <- dplyr::filter(geo_df, ip_version == 4L, !is.na(ip_start_int), !is.na(ip_end_int))

  # Use an in-memory DuckDB instance for the inequality join.
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbWriteTable(con, "asn", asn_df, overwrite = TRUE)
  DBI::dbWriteTable(con, "geo", geo_df, overwrite = TRUE)

  # Point lookup by ASN range start.
  # This is intentionally simple and reproducible (fast enough for course projects).
  geo_cols <- names(geo_df)
  select_parts <- c("a.*")
  if ("country_code" %in% geo_cols) select_parts <- c(select_parts, "g.country_code AS geo_country_code")
  if (identical(geo, "city")) {
    if ("city" %in% geo_cols) select_parts <- c(select_parts, "g.city AS geo_city")
    if ("latitude" %in% geo_cols) select_parts <- c(select_parts, "g.latitude AS geo_latitude")
    if ("longitude" %in% geo_cols) select_parts <- c(select_parts, "g.longitude AS geo_longitude")
  }
  select_parts <- c(select_parts, "g.ip_start AS geo_ip_start", "g.ip_end AS geo_ip_end")

  q <- paste0(
    "SELECT ", paste(select_parts, collapse = ", "),
    " FROM asn a ",
    "LEFT JOIN geo g ",
    "ON g.ip_start_int <= a.ip_start_int AND g.ip_end_int >= a.ip_start_int"
  )

  out <- DBI::dbGetQuery(con, q)
  out <- tibble::as_tibble(out)
  attr(out, "dbip_versions") <- list(asn = ver_asn, geo_database = geo, geo = ver_geo)
  attr(out, "dbip_version_label") <- paste0("asn-", ver_asn, "__", geo, "-", ver_geo)
  out
}


