#' Find the most recent available DB-IP "lite" dataset version
#'
#' DB-IP free datasets are typically published monthly. This function probes a
#' configurable number of previous months to find the latest version that
#' returns HTTP 200 for the expected download URL.
#'
#' @param database One of `"country"`, `"city"`, `"asn"`.
#' @param n_back How many months back (including current month) to probe.
#' @param base_url Base URL for DB-IP free downloads.
#'
#' @return A version string like `"2025-12"`.
#' @export
dbip_latest_version <- function(
    database = c("country", "city", "asn"),
    n_back = 24,
    base_url = "https://download.db-ip.com/free"
) {
  check_installed(c("httr2", "stringr"))
  database <- match.arg(database)
  if (!is.numeric(n_back) || length(n_back) != 1 || n_back < 1) {
    abort_pkg("`n_back` must be a single positive number.")
  }

  # Candidate versions: current month, then going back month-by-month.
  first_of_month <- as.Date(format(Sys.Date(), "%Y-%m-01"))
  candidates <- format(seq(from = first_of_month, by = "-1 month", length.out = n_back), "%Y-%m")

  for (ver in candidates) {
    url <- dbip_build_url(database = database, version = ver, base_url = base_url)
    if (dbip_url_exists(url)) return(ver)
  }

  abort_pkg(
    paste0(
      "Could not find an available DB-IP dataset for `database = \"",
      database,
      "\"` by probing the last ",
      n_back,
      " months.\n",
      "If you know a working URL, pass it via `base_url`/`database` or download manually."
    )
  )
}

#' @keywords internal
dbip_build_url <- function(database, version, base_url) {
  paste0(base_url, "/dbip-", database, "-lite-", version, ".csv.gz")
}

#' @keywords internal
dbip_url_exists <- function(url) {
  # HEAD sometimes blocked; fall back to GET with small range.
  req <- httr2::request(url) |>
    httr2::req_method("HEAD") |>
    httr2::req_timeout(10)

  ok <- tryCatch({
    resp <- httr2::req_perform(req)
    httr2::resp_status(resp) < 400
  }, error = function(e) FALSE)

  if (ok) return(TRUE)

  # Fallback: GET first byte (Range) to check existence.
  req2 <- httr2::request(url) |>
    httr2::req_headers(Range = "bytes=0-0") |>
    httr2::req_timeout(10)

  tryCatch({
    resp2 <- httr2::req_perform(req2)
    httr2::resp_status(resp2) < 400
  }, error = function(e) FALSE)
}


