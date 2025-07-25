from typing import Any

from dlt_dbt_dagster.utils.processing_utils import add_year_month, get_month_range, keep_specific_columns


class TestProcessingUtils:
    """Test utility functions for data processing"""

    def test_keep_specific_columns_with_valid_columns(self) -> None:
        """Test keep_specific_columns with valid column names"""
        record = {"id": 1, "name": "Test", "details": "Some details", "extra": "data"}
        columns_to_keep = ["id", "name"]

        result_func = keep_specific_columns(columns_to_keep)
        result = result_func(record)

        expected: dict[str, Any] = {"id": 1, "name": "Test"}
        assert result == expected

    def test_keep_specific_columns_with_missing_columns(self) -> None:
        """Test keep_specific_columns with columns that don't exist in record"""
        record = {"id": 1, "name": "Test"}
        columns_to_keep = ["id", "name", "nonexistent"]

        result_func = keep_specific_columns(columns_to_keep)
        result = result_func(record)

        expected: dict[str, Any] = {"id": 1, "name": "Test"}
        assert result == expected

    def test_keep_specific_columns_with_none(self) -> None:
        """Test keep_specific_columns with None columns list"""
        record = {"id": 1, "name": "Test"}

        result_func = keep_specific_columns(None)
        result = result_func(record)

        expected: dict[str, Any] = {}
        assert result == expected

    def test_keep_specific_columns_with_empty_list(self) -> None:
        """Test keep_specific_columns with empty columns list"""
        record = {"id": 1, "name": "Test"}

        result_func = keep_specific_columns([])
        result = result_func(record)

        expected: dict[str, Any] = {}
        assert result == expected

    def test_get_month_range_regular_month(self) -> None:
        """Test get_month_range for a regular month (not December)"""
        year, month = 2021, 3

        start_date, end_date = get_month_range(year, month)

        expected_start = "2021-03-01T00:00:00Z"
        expected_end = "2021-04-01T00:00:00Z"

        assert start_date == expected_start
        assert end_date == expected_end

    def test_get_month_range_december(self) -> None:
        """Test get_month_range for December (edge case)"""
        year, month = 2021, 12

        start_date, end_date = get_month_range(year, month)

        expected_start = "2021-12-01T00:00:00Z"
        expected_end = "2022-01-01T00:00:00Z"

        assert start_date == expected_start
        assert end_date == expected_end

    def test_add_year_month(self) -> None:
        """Test add_year_month function"""
        record = {"id": 1, "name": "Test"}
        year, month = 2021, 3

        result_func = add_year_month(year, month)
        result = result_func(record)

        expected: dict[str, Any] = {"id": 1, "name": "Test", "year": 2021, "month": 3}
        assert result == expected

    def test_add_year_month_preserves_existing_data(self) -> None:
        """Test add_year_month preserves existing record data"""
        record = {"id": 1, "name": "Test", "year": 2020, "month": 1}
        year, month = 2021, 3

        result_func = add_year_month(year, month)
        result = result_func(record)

        expected: dict[str, Any] = {"id": 1, "name": "Test", "year": 2021, "month": 3}
        assert result == expected

    def test_keep_specific_columns_with_fixture(self, sample_launch_record: dict[str, Any]) -> None:
        """Test keep_specific_columns using the fixture"""
        columns_to_keep = ["id", "name", "flight_number"]

        result_func = keep_specific_columns(columns_to_keep)
        result = result_func(sample_launch_record)

        expected: dict[str, Any] = {"id": "test_id_123", "name": "Test Launch", "flight_number": 100}
        assert result == expected
