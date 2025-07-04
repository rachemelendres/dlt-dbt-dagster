"""This script holds all utility functions used to process extracted SpaceX API data"""

from datetime import datetime
from typing import Callable, Optional


def keep_specific_columns(columns_to_keep: Optional[list[str]] = None) -> Callable[[dict], dict]:
    """Keep only the specified columns in the document"""

    def inner_func(record: dict) -> dict:
        cols = columns_to_keep if columns_to_keep is not None else []
        record_with_cols_to_keep = {}
        for column_name in cols:
            if column_name in record:
                record_with_cols_to_keep[column_name] = record[column_name]
        return record_with_cols_to_keep

    return inner_func


def get_month_range(year: int, month: int) -> tuple[str, str]:
    """Get the start and end dates for a given month"""
    start = datetime(year, month, 1)
    end = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
    return start.isoformat() + "Z", end.isoformat() + "Z"


def add_year_month(year: int, month: int) -> Callable[[dict], dict]:
    """Add the year and month to the record"""

    def inner_func(record: dict) -> dict:
        record["year"] = year
        record["month"] = month
        return record

    return inner_func
