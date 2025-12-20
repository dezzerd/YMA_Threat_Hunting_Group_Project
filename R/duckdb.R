#' Write a dataset into DuckDB
#'
#' Convenience helper around `DBI::dbWriteTable()` using the `duckdb` backend.
#'
#' @param df A data frame.
#' @param db_path Path to a DuckDB database file (`.duckdb`).
#' @param table Table name.
#' @param overwrite Overwrite existing table.
#'
#' @return Invisibly, `TRUE` on success.
#' @export
dbip_write_duckdb <- function(df, db_path, table = "dbip", overwrite = TRUE) {
  check_installed(c("DBI", "duckdb"))
  if (!is.character(db_path) || length(db_path) != 1) abort_pkg("`db_path` must be a single path.")
  if (!is.character(table) || length(table) != 1) abort_pkg("`table` must be a single string.")

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbWriteTable(con, name = table, value = df, overwrite = overwrite)
  invisible(TRUE)
}


