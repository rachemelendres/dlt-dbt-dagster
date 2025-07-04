# SpaceX API base URL and endpoints
BASE_URL = "https://api.spacexdata.com/v4/"

RESOURCES = ["launches", "rockets", "launchpads", "payloads", "cores", "ships"]

ENDPOINTS = {resource: f"{resource}/query" for resource in RESOURCES}
