# SpaceX API base URL and endpoints
from enum import Enum

BASE_URL = "https://api.spacexdata.com/v4/"


class Endpoints(str, Enum):
    """Enum for SpaceX API endpoints"""

    LAUNCHES = "launches/query"
    ROCKETS = "rockets/query"
    LAUNCHPADS = "launchpads/query"
    PAYLOADS = "payloads/query"
    CORES = "cores/query"
    SHIPS = "ships/query"
