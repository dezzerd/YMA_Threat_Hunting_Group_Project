#' Read a DB-IP "lite" dataset into a tibble
#'
#' Reads a downloaded `.csv.gz` file and normalizes the output to include
#' `ip_start`, `ip_end`, and helper columns `ip_version`, `ip_start_int`,
#' `ip_end_int` (IPv4 only).
#'
#' @param path Path to a `.csv.gz` file produced by [dbip_download()] or a
#'   compatible DB-IP CSV export.
#'
#' @return A tibble.
#' @export
dbip_read <- function(path) {
  check_installed(c("readr", "dplyr", "tibble", "stringr"))
  if (!file.exists(path)) abort_pkg(paste0("File not found: ", path))

  read_one <- function(col_names = TRUE) {
    readr::read_csv(
      path,
      col_names = col_names,
      show_col_types = FALSE,
      progress = FALSE,
      col_types = readr::cols(.default = readr::col_character())
    )
  }

  df <- read_one(col_names = TRUE)

  # DB-IP columns are typically: start_ip, end_ip, ... (country/city/asn fields).
  # Normalize column names.
  normalize_names <- function(x) {
    nm <- names(x)
    nm <- stringr::str_to_lower(nm)
    nm <- stringr::str_replace_all(nm, "[^a-z0-9]+", "_")
    names(x) <- nm
    x
  }

  df <- normalize_names(df)

  # Known variants.
  if ("start_ip" %in% names(df) && !"ip_start" %in% names(df)) df <- dplyr::rename(df, ip_start = start_ip)
  if ("end_ip" %in% names(df) && !"ip_end" %in% names(df)) df <- dplyr::rename(df, ip_end = end_ip)

  # Some DB-IP free "lite" downloads come without header.
  # If we don't see required columns, re-read with `col_names = FALSE`
  # and assign names by the most common DB-IP formats.
  if (!all(c("ip_start", "ip_end") %in% names(df))) {
    df2 <- read_one(col_names = FALSE)
    n <- ncol(df2)
    if (n < 2) abort_pkg("DB-IP file must contain at least 2 columns (ip_start, ip_end).")

    # Assign best-effort names by column count.
    if (n == 3) {
      names(df2) <- c("ip_start", "ip_end", "country_code")
    } else if (n == 4) {
      # Most commonly: ASN lite -> start, end, as_number, name
      names(df2) <- c("ip_start", "ip_end", "as_number", "name")
    } else if (n == 6) {
      # Common city-like format (no region): start, end, country, city, lat, lon
      names(df2) <- c("ip_start", "ip_end", "country_code", "city", "latitude", "longitude")
    } else if (n == 7) {
      # Common city-like format (with region): start, end, country, region, city, lat, lon
      names(df2) <- c("ip_start", "ip_end", "country_code", "region", "city", "latitude", "longitude")
    } else {
      names(df2) <- c("ip_start", "ip_end", paste0("v", seq_len(n - 2) + 2))
    }

    df <- normalize_names(df2)
    if ("start_ip" %in% names(df) && !"ip_start" %in% names(df)) df <- dplyr::rename(df, ip_start = start_ip)
    if ("end_ip" %in% names(df) && !"ip_end" %in% names(df)) df <- dplyr::rename(df, ip_end = end_ip)
  }

  if (!all(c("ip_start", "ip_end") %in% names(df))) {
    abort_pkg("DB-IP file must contain `ip_start`/`ip_end` (or `start_ip`/`end_ip`) columns.")
  }

  df <- dplyr::mutate(
    df,
    ip_version = dplyr::if_else(is_ipv6(ip_start), 6L, 4L),
    ip_start_int = dplyr::if_else(ip_version == 4L, ipv4_to_uint32(ip_start), NA_real_),
    ip_end_int = dplyr::if_else(ip_version == 4L, ipv4_to_uint32(ip_end), NA_real_),
    ip_n = dplyr::if_else(
      ip_version == 4L & !is.na(ip_start_int) & !is.na(ip_end_int),
      (ip_end_int - ip_start_int + 1),
      NA_real_
    ),
    cidr = dplyr::if_else(
      ip_version == 4L,
      ipv4_range_to_cidr(ip_start, ip_start_int, ip_end_int),
      NA_character_
    )
  )

  tibble::as_tibble(df)
}


