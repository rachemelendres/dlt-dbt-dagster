"""Constants for selected columns per endpoint in the SpaceX API pipeline"""

from enum import Enum
from typing import Any


class BronzeSchema(str, Enum):
    """Enum for bronze layer schema definitions with validation"""

    LAUNCHES = "bronze_launches"
    ROCKETS = "bronze_rockets"
    LAUNCHPADS = "bronze_launchpads"
    PAYLOADS = "bronze_payloads"
    CORES = "bronze_cores"
    SHIPS = "bronze_ships"

    @classmethod
    def __getattr__(cls, name: str) -> Any:
        """Catch attempts to access non-existent enum members at class level"""
        raise AttributeError(  # noqa: TRY003
            f"'{cls.__name__}' has no attribute '{name}'. Available members: {[member.name for member in cls]}"
        )

    @classmethod
    def get_columns(cls, schema_type: "BronzeSchema") -> list[str]:
        """Get the column list for a specific schema type"""
        column_mapping = {
            cls.LAUNCHES: [
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
            ],
            cls.ROCKETS: [
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
            ],
            cls.LAUNCHPADS: [
                "id",
                "name",
                "full_name",
                "locality",
                "region",
                "latitude",
                "longitude",
                "launch_attempts",
                "launch_successes",
                "launches",
                "rockets",
                "status",
                "details",
                "timezone",
            ],
            cls.PAYLOADS: [
                "id",
                "name",
                "type",
                "launch",
                "reused",
                "manufacturers",
                "customers",
                "nationalities",
                "mass_kg",
                "orbit",
                "lifespan_years",
                "epoch",
            ],
            cls.SHIPS: [
                "id",
                "name",
                "model",
                "type",
                "active",
                "mass_kg",
                "year_built",
                "home_port",
                "launches",
            ],
            cls.CORES: [
                "id",
                "status",
                "serial",
                "reuse_count",
                "launches",
                "last_update",
            ],
        }

        if schema_type not in column_mapping:
            raise ValueError(f"Unknown schema type: {schema_type}. Available types: {list(column_mapping.keys())}")  # noqa: TRY003

        return column_mapping[schema_type]
