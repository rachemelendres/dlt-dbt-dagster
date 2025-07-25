"""Unit tests for SpaceX pipeline functionality"""

from unittest.mock import Mock, patch

import pytest

from dlt_dbt_dagster.constants.endpoints import BASE_URL, Endpoints
from dlt_dbt_dagster.constants.schema.bronze import BronzeSchema
from dlt_dbt_dagster.dlt.spacex_pipeline import MONTH, YEAR, load_spacex_bronze_data, spacex_api_source


class TestSpaceXPipeline:
    """Test main SpaceX pipeline functions"""

    def test_launches_columns_structure(self) -> None:
        """Test that BronzeSchema.get_columns(BronzeSchema.LAUNCHES) contains expected columns"""
        # Get the actual columns from BronzeSchema
        launch_columns = BronzeSchema.get_columns(BronzeSchema.LAUNCHES)

        expected_columns = [
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

        assert set(expected_columns).issubset(set(launch_columns))
        assert len(launch_columns) >= 11  # At least these columns should be present

    def test_constants_values(self) -> None:
        """Test that YEAR and MONTH constants have expected values"""
        assert YEAR == 2021
        assert MONTH == 3

    @patch("dlt_dbt_dagster.dlt.spacex_pipeline.rest_api_resources")
    def test_spacex_api_source_config_structure(self, mock_rest_api_resources: Mock) -> None:
        """Test that spacex_api_source returns correct config structure"""
        mock_rest_api_resources.return_value = []

        # Get the generator
        source_generator = spacex_api_source(year=2021, month=3)

        # This will trigger the function to execute and create the config
        list(source_generator)

        # Verify rest_api_resources was called
        mock_rest_api_resources.assert_called_once()

        # Get the config that was passed to rest_api_resources
        config = mock_rest_api_resources.call_args[0][0]

        # Test client configuration
        assert config["client"]["base_url"] == BASE_URL

        # Test resource defaults
        resource_defaults = config["resource_defaults"]
        assert resource_defaults["endpoint"]["method"] == "POST"
        assert resource_defaults["endpoint"]["data_selector"] == "docs[*]"

        # Test resources configuration
        resources = config["resources"]
        assert len(resources) >= 1  # Ensure we have at least the launches resource

        launches_resource = resources[0]
        assert launches_resource["name"] == BronzeSchema.LAUNCHES
        assert launches_resource["endpoint"]["path"] == Endpoints.LAUNCHES
        assert launches_resource["primary_key"] == ["id"]
        assert launches_resource["merge_key"] == ["year", "month"]
        assert launches_resource["write_disposition"] == "merge"

    @patch("dlt_dbt_dagster.dlt.spacex_pipeline.rest_api_resources")
    def test_spacex_api_source_date_range(self, mock_rest_api_resources: Mock) -> None:
        """Test that spacex_api_source uses correct date range"""
        mock_rest_api_resources.return_value = []

        source_generator = spacex_api_source(year=2021, month=3)
        list(source_generator)

        config = mock_rest_api_resources.call_args[0][0]
        query = config["resources"][0]["endpoint"]["json"]["query"]

        expected_start = "2021-03-01T00:00:00Z"
        expected_end = "2021-04-01T00:00:00Z"

        assert query["date_utc"]["$gte"] == expected_start
        assert query["date_utc"]["$lt"] == expected_end

    @patch("dlt_dbt_dagster.dlt.spacex_pipeline.rest_api_resources")
    def test_spacex_api_source_processing_steps(self, mock_rest_api_resources: Mock) -> None:
        """Test that spacex_api_source includes correct processing steps"""
        mock_rest_api_resources.return_value = []

        source_generator = spacex_api_source(year=2021, month=3)
        list(source_generator)

        config = mock_rest_api_resources.call_args[0][0]
        processing_steps = config["resources"][0]["processing_steps"]

        assert len(processing_steps) == 2

        # Test first processing step (keep_specific_columns)
        first_step = processing_steps[0]
        assert "map" in first_step

        # Test second processing step (add_year_month)
        second_step = processing_steps[1]
        assert "map" in second_step

    @patch("dlt_dbt_dagster.dlt.spacex_pipeline.dlt.pipeline")
    @patch("dlt_dbt_dagster.dlt.spacex_pipeline.spacex_api_source")
    def test_load_spacex_bronze_data_pipeline_config(self, mock_source: Mock, mock_pipeline_class: Mock) -> None:
        """Test that load_spacex_bronze_data creates pipeline with correct config"""
        # Mock the pipeline instance
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        mock_pipeline.run.return_value = Mock()

        # Mock the source
        mock_source.return_value = []

        load_spacex_bronze_data(year=2021, month=3)

        # Verify pipeline was created with correct parameters
        mock_pipeline_class.assert_called_once_with(
            pipeline_name="spacex_api",
            destination="duckdb",
            dataset_name="bronze",
            progress="log",
            dev_mode=True,
        )

        # Verify pipeline.run was called
        mock_pipeline.run.assert_called_once()

    @patch("dlt_dbt_dagster.dlt.spacex_pipeline.dlt.pipeline")
    @patch("dlt_dbt_dagster.dlt.spacex_pipeline.spacex_api_source")
    def test_load_spacex_bronze_data_source_called(self, mock_source: Mock, mock_pipeline_class: Mock) -> None:
        """Test that load_spacex_bronze_data calls spacex_api_source with correct parameters"""
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        mock_pipeline.run.return_value = Mock()

        mock_source.return_value = []

        load_spacex_bronze_data(year=2021, month=3)

        # Verify spacex_api_source was called with correct parameters
        mock_source.assert_called_once_with(year=2021, month=3)

        # Verify the source was passed to pipeline.run
        mock_pipeline.run.assert_called_once_with(mock_source.return_value)

    def test_load_spacex_bronze_data_with_fixtures(self, mock_dlt_pipeline: Mock) -> None:
        """Test load_spacex_bronze_data using fixtures"""
        with (
            patch("dlt_dbt_dagster.dlt.spacex_pipeline.dlt.pipeline", return_value=mock_dlt_pipeline),
            patch("dlt_dbt_dagster.dlt.spacex_pipeline.spacex_api_source", return_value=[]),
        ):
            load_spacex_bronze_data(year=2021, month=3)

            # Verify the mock pipeline was used
            mock_dlt_pipeline.run.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__])
