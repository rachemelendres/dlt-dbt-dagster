from typing import Any, Optional

from dlt.sources.helpers.requests import Request, Response
from dlt.sources.helpers.rest_client.paginators import BasePaginator


class CustomJsonPaginator(BasePaginator):
    """Implements a custom paginator for the SpaceX API pipeline"""

    def __init__(self, page_json_body: str = "page", initial_page: int = 1):
        super().__init__()
        self.page_json_body = page_json_body
        self.page = initial_page

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
        request.json["options"][self.page_json_body] = self.page
