"""This script extracts SpaceX API data and loads it into a local DuckDB database. It's designed for local development and testing purposes only."""

from typing import Any

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from dotenv import load_dotenv

from dlt_dbt_dagster.constants.endpoints import BASE_URL, ENDPOINTS
from dlt_dbt_dagster.dlt.custom_paginator import CustomJsonPaginator
from dlt_dbt_dagster.utils.processing_utils import add_year_month, get_month_range, keep_specific_columns

load_dotenv()

YEAR = 2021
MONTH = 3

LAUNCHES_SELECTED_COLUMNS = [
    "id",
    "name",
    "details",
    "flight_number",
    "launchpad",
    "date_utc",
    "rocket",
    "payloads",
    "ships",
    "cores",
    "success",
]

ROCKETS_SELECTED_COLUMNS = [
    "id",
    "name",
    "description",
    "company",
    "type",
    "active",
    "stages",
    "boosters",
    "cost_per_launch",
    "success_rate_pct",
    "first_flight",
    "country",
]


@dlt.source(
    name="spacex_api_source",
    max_table_nesting=0,
    schema_contract={"tables": "evolve", "columns": "discard_value", "data_type": "freeze"},
)
def spacex_api_source(year: int, month: int) -> Any:
    """Make the SpaceX API source"""
    start_date, end_date = get_month_range(year, month)
    rest_api_config: RESTAPIConfig = {
        "client": {
            "base_url": BASE_URL,
            "paginator": CustomJsonPaginator(),
        },
        "resource_defaults": {
            "primary_key": "id",
            "merge_key": "id",
            "write_disposition": {"disposition": "merge", "strategy": "scd2"},
            "endpoint": {
                "method": "POST",
                "json": {
                    "query": {},
                    "options": {"limit": 50},
                },
                "data_selector": "docs[*]",
            },
        },
        "resources": [
            {
                "primary_key": ["id"],
                "merge_key": ["year", "month"],
                "columns": {"date_utc": {"dedup_sort": "desc"}},
                "write_disposition": "merge",
                "name": "bronze_launches",
                "endpoint": {
                    "path": ENDPOINTS["launches"],
                    "json": {
                        "query": {"date_utc": {"$gte": start_date, "$lt": end_date}},
                        "options": {"limit": 50},
                    },
                },
                "processing_steps": [
                    {"map": keep_specific_columns(columns_to_keep=LAUNCHES_SELECTED_COLUMNS)},  # type: ignore[typeddict-item]
                    {"map": add_year_month(year=year, month=month)},  # type: ignore[typeddict-item]
                ],
            },
            {
                "name": "bronze_rockets",
                "endpoint": {
                    "path": ENDPOINTS["rockets"],
                },
                "processing_steps": [
                    {"map": keep_specific_columns(columns_to_keep=ROCKETS_SELECTED_COLUMNS)},  # type: ignore[typeddict-item]
                ],
            },
        ],
    }
    yield from rest_api_resources(rest_api_config)


def load_spacex_bronze_data(year: int, month: int) -> None:
    """Load monthly SpaceX API bronze data to DuckDB"""

    pipeline = dlt.pipeline(
        pipeline_name="spacex_api",
        destination="duckdb",
        dataset_name="bronze",
        progress="log",
        dev_mode=True,
    )
    load_info = pipeline.run(spacex_api_source(year=year, month=month))
    print(load_info)


if __name__ == "__main__":
    load_spacex_bronze_data(year=YEAR, month=MONTH)
