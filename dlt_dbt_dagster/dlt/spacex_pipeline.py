"""This script extracts SpaceX API data and loads it into a local DuckDB database. It's designed for local development and testing purposes only."""

from typing import Any

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from dotenv import load_dotenv

from dlt_dbt_dagster.constants.endpoints import BASE_URL, Endpoints
from dlt_dbt_dagster.constants.schema.bronze import BronzeSchema
from dlt_dbt_dagster.dlt.custom_paginator import CustomJsonPaginator
from dlt_dbt_dagster.utils.processing_utils import add_year_month, get_month_range, keep_specific_columns

load_dotenv()

YEAR = 2021
MONTH = 3


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
                "merge_key": ["year", "month"],
                "write_disposition": {"disposition": "merge", "strategy": "delete-insert"},
                "name": BronzeSchema.LAUNCHES,
                "endpoint": {
                    "path": Endpoints.LAUNCHES,
                    "json": {
                        "query": {"date_utc": {"$gte": start_date, "$lt": end_date}},
                        "options": {"limit": 50},
                    },
                },
                "processing_steps": [
                    {"map": keep_specific_columns(columns_to_keep=BronzeSchema.get_columns(BronzeSchema.LAUNCHES))},  # type: ignore[typeddict-item]
                    {"map": add_year_month(year=year, month=month)},  # type: ignore[typeddict-item]
                ],
            },
            {
                "name": BronzeSchema.ROCKETS,
                "endpoint": {
                    "path": Endpoints.ROCKETS,
                },
                "processing_steps": [
                    {"map": keep_specific_columns(columns_to_keep=BronzeSchema.get_columns(BronzeSchema.ROCKETS))},  # type: ignore[typeddict-item]
                ],
            },
            {
                "name": BronzeSchema.CORES,
                "endpoint": {
                    "path": Endpoints.CORES,
                },
                "processing_steps": [
                    {"map": keep_specific_columns(columns_to_keep=BronzeSchema.get_columns(BronzeSchema.CORES))},  # type: ignore[typeddict-item]
                ],
            },
            {
                "name": BronzeSchema.PAYLOADS,
                "endpoint": {
                    "path": Endpoints.PAYLOADS,
                },
                "columns": {
                    "mass_kg": {"data_type": "double"},
                    "lifespan_years": {"data_type": "double"},
                },
                "processing_steps": [
                    {"map": keep_specific_columns(columns_to_keep=BronzeSchema.get_columns(BronzeSchema.PAYLOADS))},  # type: ignore[typeddict-item]
                ],
            },
            {
                "name": BronzeSchema.LAUNCHPADS,
                "endpoint": {
                    "path": Endpoints.LAUNCHPADS,
                },
                "processing_steps": [
                    {"map": keep_specific_columns(columns_to_keep=BronzeSchema.get_columns(BronzeSchema.LAUNCHPADS))},  # type: ignore[typeddict-item]
                ],
            },
            {
                "name": BronzeSchema.SHIPS,
                "endpoint": {
                    "path": Endpoints.SHIPS,
                },
                "processing_steps": [
                    {"map": keep_specific_columns(columns_to_keep=BronzeSchema.get_columns(BronzeSchema.SHIPS))},  # type: ignore[typeddict-item]
                ],
            },
        ],
    }
    yield from rest_api_resources(rest_api_config)


def load_spacex_bronze_data(year: int, month: int) -> None:
    """Load monthly SpaceX API bronze data to DuckDB"""

    pipeline = dlt.pipeline(
        pipeline_name="dev",
        destination="duckdb",
        dataset_name="bronze",
        progress="log",
        dev_mode=True,
    )
    load_info = pipeline.run(spacex_api_source(year=year, month=month))
    print(load_info)


if __name__ == "__main__":
    load_spacex_bronze_data(year=YEAR, month=MONTH)
