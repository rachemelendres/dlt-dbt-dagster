"""Constants for table names used in the data pipeline"""

from enum import Enum


class BronzeTables(str, Enum):
    """Enum for bronze layer table names"""

    LAUNCHES = "bronze_launches"
    ROCKETS = "bronze_rockets"
    LAUNCHPADS = "bronze_launchpads"
    PAYLOADS = "bronze_payloads"
    CORES = "bronze_cores"
    SHIPS = "bronze_ships"
