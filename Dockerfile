FROM rocker/r-ver:4.4.2

ENV DEBIAN_FRONTEND=noninteractive
ENV CRAN=https://packagemanager.posit.co/cran/__linux__/noble/latest

# System deps for common R packages (httr2/curl, ssl, xml, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/GroupProject
COPY . /opt/GroupProject

# Install deps + the local package
RUN R -q -e "install.packages(c('httr2','readr','dplyr','tibble','stringr','purrr','rlang','DBI','duckdb','shiny','leaflet','ggplot2'), repos = Sys.getenv('CRAN'))"
RUN R CMD INSTALL .

EXPOSE 3838

CMD ["R", "-q", "-e", "GroupProject::run_dbip_viewer(host='0.0.0.0', port=3838, launch.browser=FALSE)"]


