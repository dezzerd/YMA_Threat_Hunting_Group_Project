#' Download a DB-IP "lite" dataset file
#'
#' Downloads the requested DB-IP free dataset (`country`, `city`, or `asn`, if
#' available) into the package cache directory. By default it will attempt to
#' find the latest available monthly version.
#'
#' @param database One of `"country"`, `"city"`, `"asn"`.
#' @param version `"latest"` or a `"YYYY-MM"` string.
#' @param dest_dir Where to store the downloaded file.
#' @param overwrite Whether to overwrite an existing file.
#' @param base_url Base URL for DB-IP free downloads.
#'
#' @return Path to the downloaded `.csv.gz` file.
#' @export
dbip_download <- function(
    database = c("country", "city", "asn"),
    version = "latest",
    dest_dir = dbip_cache_dir(),
    overwrite = FALSE,
    base_url = "https://download.db-ip.com/free"
) {
  check_installed(c("httr2"))
  database <- match.arg(database)
  if (!dir.exists(dest_dir)) dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)

  if (identical(version, "latest")) {
    version <- dbip_latest_version(database = database, base_url = base_url)
  }

  url <- dbip_build_url(database = database, version = version, base_url = base_url)
  dest <- file.path(dest_dir, paste0("dbip-", database, "-lite-", version, ".csv.gz"))
  meta <- file.path(dest_dir, paste0("dbip-", database, "-lite-", version, ".meta"))

  if (file.exists(dest) && !isTRUE(overwrite)) {
    return(dest)
  }

  req <- httr2::request(url) |>
    httr2::req_timeout(120) |>
    httr2::req_error(is_error = function(resp) httr2::resp_status(resp) >= 400)

  httr2::req_perform(req, path = dest)

  # Write minimal metadata for reproducibility.
  md <- c(
    paste0("url=", url),
    paste0("downloaded_at=", format(Sys.time(), "%Y-%m-%d %H:%M:%S %z")),
    paste0("database=", database),
    paste0("version=", version)
  )
  writeLines(md, con = meta, useBytes = TRUE)

  dest
}


