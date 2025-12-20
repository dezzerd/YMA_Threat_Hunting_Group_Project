# GroupProject — Reproducible DB-IP ETL (R package) + Shiny viewer

Этот репозиторий содержит R-пакет `GroupProject`, реализующий воспроизводимый
ETL-процесс для DB-IP "lite" датасетов (страна/город/ASN) и минимальное Shiny-приложение
для просмотра результатов.

## Быстрый старт (R)

1) Установите зависимости и пакет (в RStudio: `Ctrl+Shift+B`) или:

```r
install.packages(c("httr2","readr","dplyr","tibble","stringr","purrr","rlang","DBI","duckdb","shiny"))
devtools::install(".")
```

2) Запустите ETL:

```r
library(GroupProject)

# download -> read -> transform -> save .rds (+ optional DuckDB)
df_country <- dbip_etl("country", version = "latest", write_rds = TRUE, write_duckdb = TRUE)
df_asn_geo <- dbip_etl("asn_geo", geo = "country", version = "latest", write_rds = TRUE, write_duckdb = TRUE)
```

3) Запустите витрину (Shiny):

```r
GroupProject::run_dbip_viewer(host = "127.0.0.1", port = 3838)
```

## data-raw (скрипты ETL)

См. `data-raw/` — там находится воспроизводимый скрипт запуска пайплайна:
`data-raw/01_dbip_build_artifacts.R`.

## Docker

Сборка и запуск:

```bash
docker compose up --build
```

После запуска приложение доступно на `http://localhost:3838`.


