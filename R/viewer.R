#' Run the DB-IP Shiny viewer
#'
#' Launches the Shiny app shipped with this package. The app can trigger the ETL
#' pipeline and explore the resulting data.
#'
#' @param host Shiny host.
#' @param port Shiny port.
#' @param launch.browser Whether to open a browser automatically.
#'
#' @return Invisibly, the Shiny app object.
#' @export
run_dbip_viewer <- function(host = "127.0.0.1", port = 3838, launch.browser = TRUE) {
  check_installed(c("shiny"))
  app_dir <- system.file("shiny/dbip_viewer", package = "GroupProject")
  if (identical(app_dir, "")) abort_pkg("Shiny app directory not found in installed package.")
  shiny::runApp(appDir = app_dir, host = host, port = port, launch.browser = launch.browser)
}


