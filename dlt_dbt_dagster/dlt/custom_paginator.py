from typing import Any, Optional

from dlt.sources.helpers.requests import Request, Response
from dlt.sources.helpers.rest_client.paginators import BasePaginator


class CustomJsonPaginator(BasePaginator):
    """Implements a custom paginator for the SpaceX API pipeline"""

    def __init__(self, initial_page: int = 1, options_key: str = "options", page_key: str = "page"):
        super().__init__()
        self.page = initial_page
        self.options_key = options_key
        self.page_key = page_key

    def init_request_json(self, request: Request) -> None:  # type: ignore[no-any-unimported]
        """Sets up the initial API request JSON body parameters"""
        self.update_request(request)

    def update_state(self, response: Response, data: Optional[list[Any]] = None) -> None:  # type: ignore[no-any-unimported]
        """Updates the paginator's state based on the response of API call"""
        # Check the 'hasNextPage' field in the JSON response to determine if need to continue to next page or not
        if response.json().get("hasNextPage"):
            self.page += 1
        else:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:  # type: ignore[no-any-unimported]
        """Modifies the API request JSON body parameters based on the current state of paginator"""

        if request.json is None:
            request.json = {}

        # Ensure the `options` key exists
        if self.options_key not in request.json:
            request.json[self.options_key] = {}

        request.json[self.options_key][self.page_key] = self.page
