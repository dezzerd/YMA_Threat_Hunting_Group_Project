#' Default cache directory for DB-IP downloads
#'
#' Returns a user-specific cache directory used by this package to store
#' downloaded DB-IP files (e.g., `.csv.gz`).
#'
#' @return A single path.
#' @export
dbip_cache_dir <- function() {
  dir <- tools::R_user_dir("GroupProject", which = "cache")
  if (!dir.exists(dir)) dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  dir
}

#' Default data directory for transformed datasets
#'
#' Returns a user-specific data directory used by this package to store
#' transformed outputs (e.g., `.rds`, DuckDB files).
#'
#' @return A single path.
#' @export
dbip_data_dir <- function() {
  dir <- tools::R_user_dir("GroupProject", which = "data")
  if (!dir.exists(dir)) dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  dir
}


