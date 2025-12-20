# Reproducible ETL runner (developer script)
#
# This script builds local artifacts using the package API:
# - downloads DB-IP "lite" data into the package cache directory
# - writes tidy outputs into the package data directory (`tools::R_user_dir()`)
# - optionally writes tables into DuckDB for downstream analysis
#
# Run from the project root:
#   source("data-raw/01_dbip_build_artifacts.R")

options(stringsAsFactors = FALSE)

version <- "latest"   # or "YYYY-MM"
refresh <- FALSE      # set TRUE to force rebuild

# Build basic datasets
country <- GroupProject::dbip_etl(
  database = "country",
  version = version,
  refresh = refresh,
  write_rds = TRUE,
  write_duckdb = TRUE
)

asn_geo <- GroupProject::dbip_etl(
  database = "asn_geo",
  geo = "country",
  version = version,
  refresh = refresh,
  write_rds = TRUE,
  write_duckdb = TRUE
)

# Create a small summary table for quick exploration (optional)
if ("geo_country_code" %in% names(asn_geo) && all(c("ip_start_int", "ip_end_int") %in% names(asn_geo))) {
  asn_geo$ip_n <- (as.numeric(asn_geo$ip_end_int) - as.numeric(asn_geo$ip_start_int) + 1)
  asn_country_summary <- aggregate(ip_n ~ geo_country_code, data = asn_geo, FUN = function(v) sum(v, na.rm = TRUE))
  asn_country_summary <- asn_country_summary[order(asn_country_summary$ip_n, decreasing = TRUE), , drop = FALSE]

  out_path <- file.path(GroupProject::dbip_data_dir(), "asn_country_summary.csv")
  utils::write.csv(asn_country_summary, out_path, row.names = FALSE)
  message("Wrote summary: ", out_path)
}

invisible(list(country = country, asn_geo = asn_geo))


