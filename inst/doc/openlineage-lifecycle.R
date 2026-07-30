## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>")

## -----------------------------------------------------------------------------
library(openlineage)

transport <- AccumulatingTransport$new()
client <- OpenLineageClient$new(transport = transport, disabled = FALSE)

run <- Run(
  new_run_id(),
  facets = list(
    nominalTime = NominalTimeRunFacet(
      "2026-01-02T03:00:00.000Z",
      "2026-01-02T04:00:00.000Z"
    ),
    tags = TagsRunFacet(list(ol_tag("environment", "production")))
  )
)
job <- Job(
  "example-scheduler",
  "daily-orders",
  facets = list(
    sql = SQLJobFacet(
      "SELECT * FROM raw.orders",
      dialect = "ansi"
    )
  )
)

## -----------------------------------------------------------------------------
schema <- SchemaDatasetFacet(list(
  SchemaField("order_id", "INTEGER", ordinal_position = 1L),
  SchemaField("amount", "DECIMAL", ordinal_position = 2L)
))

input <- InputDataset(
  "postgres://warehouse",
  "raw.orders",
  facets = list(schema = schema),
  input_facets = list(
    inputStatistics = InputStatisticsInputDatasetFacet(row_count = 100L)
  )
)
output <- OutputDataset(
  "postgres://warehouse",
  "analytics.daily_orders",
  facets = list(schema = schema),
  output_facets = list(
    outputStatistics = OutputStatisticsOutputDatasetFacet(row_count = 100L)
  )
)

## -----------------------------------------------------------------------------
client$emit(RunEvent(
  run,
  job,
  event_type = "START",
  event_time = "2026-01-02T03:04:05.000Z",
  inputs = list(input)
))
client$emit(RunEvent(
  run,
  job,
  event_type = "RUNNING",
  event_time = "2026-01-02T03:04:30.000Z",
  inputs = list(input)
))
client$emit(RunEvent(
  run,
  job,
  event_type = "COMPLETE",
  event_time = "2026-01-02T03:05:00.000Z",
  inputs = list(input),
  outputs = list(output)
))

vapply(
  transport$events,
  \(event) as_openlineage_list(event)$eventType,
  character(1)
)
to_openlineage_json(transport$events[[3]], pretty = TRUE)

## -----------------------------------------------------------------------------
quality <- ol_facet(
  "https://example.com/facets/QualityRunFacet.json",
  score = 0.98,
  method = "rules"
)
extended_run <- Run(new_run_id(), facets = list(quality = quality))
as_openlineage_list(extended_run)$facets$quality$score

## -----------------------------------------------------------------------------
bearer_client <- OpenLineageClient$new(
  url = "https://lineage.example.com",
  api_key = "replace-with-your-secret",
  disabled = FALSE
)
header_client <- OpenLineageClient$new(
  url = "https://lineage.example.com",
  headers = c(`X-Tenant` = "analytics"),
  disabled = FALSE
)
bearer_client
header_client

## -----------------------------------------------------------------------------
condition <- tryCatch(
  client$emit("not a RunEvent"),
  openlineage_error = identity
)
class(condition)
conditionMessage(condition)

