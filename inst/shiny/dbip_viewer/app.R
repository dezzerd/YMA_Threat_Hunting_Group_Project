library(shiny)

ui <- fluidPage(
  titlePanel("DB-IP ETL Viewer (GroupProject)"),
  sidebarLayout(
    sidebarPanel(
      selectInput("database", "Dataset", choices = c("country", "city", "asn", "asn_geo"), selected = "country"),
      conditionalPanel(
        condition = "input.database == 'asn_geo'",
        selectInput("geo", "Geo dataset for asn_geo", choices = c("country", "city"), selected = "country")
      ),
      textInput("version", "Version", value = "latest", placeholder = "latest or YYYY-MM"),
      checkboxInput("refresh", "Refresh (re-download / rebuild)", value = FALSE),
      checkboxInput("write_duckdb", "Write to DuckDB", value = FALSE),
      actionButton("run", "Run ETL / Load", class = "btn-primary"),
      tags$hr(),
      verbatimTextOutput("status", placeholder = TRUE)
    ),
    mainPanel(
      tabsetPanel(
        tabPanel("Preview", tableOutput("preview")),
        tabPanel("Summary", plotOutput("summary_plot", height = 360)),
        tabPanel("Info", verbatimTextOutput("info"))
      )
    )
  )
)

server <- function(input, output, session) {
  data_rv <- reactiveVal(NULL)
  info_rv <- reactiveVal(list())

  output$status <- renderText({
    "Ready. Choose parameters and click 'Run ETL / Load'."
  })

  observeEvent(input$run, {
    output$status <- renderText({"Running ETL..."})

    res <- tryCatch({
      if (identical(input$database, "asn_geo")) {
        df <- GroupProject::dbip_etl(
          database = "asn_geo",
          geo = input$geo,
          version = input$version,
          refresh = isTRUE(input$refresh),
          write_rds = TRUE,
          write_duckdb = isTRUE(input$write_duckdb)
        )
      } else {
        df <- GroupProject::dbip_etl(
          database = input$database,
          version = input$version,
          refresh = isTRUE(input$refresh),
          write_rds = TRUE,
          write_duckdb = isTRUE(input$write_duckdb)
        )
      }
      list(ok = TRUE, df = df, err = NULL)
    }, error = function(e) {
      list(ok = FALSE, df = NULL, err = conditionMessage(e))
    })

    if (!isTRUE(res$ok)) {
      output$status <- renderText({paste0("Error: ", res$err)})
      return()
    }

    data_rv(res$df)
    info_rv(list(
      n_rows = nrow(res$df),
      n_cols = ncol(res$df),
      cols = names(res$df)
    ))
    output$status <- renderText({paste0("OK. Rows: ", nrow(res$df), ", Cols: ", ncol(res$df))})
  })

  output$preview <- renderTable({
    df <- data_rv()
    if (is.null(df)) return(NULL)
    utils::head(df, 30)
  })

  output$info <- renderPrint({
    info_rv()
  })

  output$summary_plot <- renderPlot({
    df <- data_rv()
    if (is.null(df)) return(invisible(NULL))

    # Prefer country-level summary if available.
    if ("country_code" %in% names(df)) {
      x <- df
      # crude IPv4 IP count; IPv6 becomes NA and is ignored
      if (all(c("ip_start_int", "ip_end_int") %in% names(x))) {
        x$ip_n <- (as.numeric(x$ip_end_int) - as.numeric(x$ip_start_int) + 1)
      } else {
        x$ip_n <- NA_real_
      }

      top <- aggregate(ip_n ~ country_code, data = x, FUN = function(v) sum(v, na.rm = TRUE))
      top <- top[order(top$ip_n, decreasing = TRUE), , drop = FALSE]
      top <- utils::head(top, 20)
      if (nrow(top) == 0) return(invisible(NULL))
      barplot(
        height = top$ip_n,
        names.arg = top$country_code,
        las = 2,
        main = "Top countries by estimated IPv4 addresses",
        ylab = "IPv4 addresses (approx.)"
      )
      return(invisible(NULL))
    }

    # asn_geo: show top by geo_country_code if present
    if ("geo_country_code" %in% names(df)) {
      x <- df
      if (all(c("ip_start_int", "ip_end_int") %in% names(x))) {
        x$ip_n <- (as.numeric(x$ip_end_int) - as.numeric(x$ip_start_int) + 1)
      } else {
        x$ip_n <- NA_real_
      }
      top <- aggregate(ip_n ~ geo_country_code, data = x, FUN = function(v) sum(v, na.rm = TRUE))
      top <- top[order(top$ip_n, decreasing = TRUE), , drop = FALSE]
      top <- utils::head(top, 20)
      if (nrow(top) == 0) return(invisible(NULL))
      barplot(
        height = top$ip_n,
        names.arg = top$geo_country_code,
        las = 2,
        main = "Top geo_country_code by estimated IPv4 addresses (asn_geo)",
        ylab = "IPv4 addresses (approx.)"
      )
      return(invisible(NULL))
    }

    plot.new()
    text(0.5, 0.5, "No supported summary columns found.")
  })
}

shinyApp(ui, server)


