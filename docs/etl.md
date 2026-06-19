# ETL for Google Ads

## Purpose

- **Extract** Google Ads data from `Google Ads API`

- **Transform** raw records into `normalized analytical schema`

- **Load** transformed data into `Google BigQuery` using idempotent `UPSERT` strategy

## Extract

- The extractor initializes Google Ads client using structured JSON credential payload

- The extractor authenticates using `developer_token`, `OAuth2 credentials` and `refresh_token`

- The extractor uses Google Ads API services to execute `GAQL` queries against the campaign resource

- The extractor transforms nested Google Ads API response objects into flattened `List[dict]` records

- The extractor uses `search_stream` API to support large-scale data extraction via streaming batches

- The extractor converts extracted records into `pandas.DataFrame`

- The extractor propagates structured error metadata using `error.retryable` flag to support orchestration-level retry logic

- The extractor enforces date filtering  in campaign insights layer using `segments.date BETWEEN start_date AND end_date`

- The extractor converts campaign insights `metrics.cost_micros` into standard currency unit by dividing by `1_000_000`

- The extractor requires `campaign_id_list` as a mandatory input for targeted metadata extraction

- The extractor builds GAQL `WHERE campaign.id IN (...)` clause dynamically based on provided campaign IDs

## Transform

- The transformer normalizes `customer_id` and `campaign_id` to `STRING` type

- The transformer enforces numeric schema for `impressions` and `clicks` as `INT64`

- The transformer converts cost into spend by multiplying by `100`, rounding, and casting to `INT64`

- The transformer enforces floating-point schema for `conversions` and `conversion_value`

- The transformer normalizes date into `UTC` timestamp and floors to daily granularity

- The transformer derives `year` dimension from normalized date

- The transformer derives `month` dimension using `YYYY-MM` format from normalized date

- The transformer enriches a constant platform column with value `Google`

- The transformer parses `campaign_name` using underscore `|` delimiter to derive structured dimensions

- The transformer fills missing parsed values with `unknown` to preserve schema consistency

## Load

- The loader uses `mode="upsert"` to support idempotent loading and deduplication

- The loader uses primary key(s) defined in `keys=[...]` to overwrite existing matching records

- The loader delegates execution to `internalGoogleBigqueryLoader` for standardized BigQuery operations

- The loader uses `keys=["date"]` to deduplicate campaign insights records at daily granularity

- The loader applies table partitioning on `partition={"field": "date"}` for campaign insights

- The loader applies table clustering on `cluster=["campaign_id"]` for campaign insights

- The loader uses composite primary keys `keys=["customer_id", "campaign_id"]` for campaign metadata

- The loader does not apply table partitioning for campaign metadata

- The loader applies table clustering on `cluster=["campaign_id"]` for campaign metadata