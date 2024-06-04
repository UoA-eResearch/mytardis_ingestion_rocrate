"""Classes and Models for Authentication and reuqests to the MyTardis API
Currently used to request and validate schemas by the extractor.
"""

import logging
from typing import Dict, Optional
from urllib.parse import urljoin

import backoff
import requests
from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import Person
from pydantic import BaseModel
from requests import Response
from requests.auth import AuthBase
from requests.exceptions import RequestException
from requests.models import PreparedRequest

from src.mt_api.api_consts import CONNECTION__HOSTNAME
from .mt_consts import UOA

logger = logging.getLogger(__name__)


class AuthConfig(BaseModel, AuthBase):
    """Attaches HTTP headers for Tastypie API key Authentication to the given

        Because this ingestion script will sit inside the private network and will
        act as the primary source for uploading to myTardis, authentication via a
        username and api key is used. The class functions to format the HTTP(S)
        header into an appropriate form for the MyTardis authentication module.

    UOA = Organisation(
        identifiers=["https://ror.org/03b94tp07"],
        name="The University of Auckland | Waipapa Taumata Rau",
        url="https://auckland.ac.nz",
        location="Auckland, New Zealand",
        research_org=True,
    )

        Attributes:
            username: str
                A MyTardis specific username. For the UoA instance this is usually a
                UPI
            api_key: str
                The API key generated through MyTardis that identifies the user with
                username
    """

    username: str
    api_key: str

    def __call__(
        self, r: PreparedRequest
    ) -> PreparedRequest:  # pylint: disable=invalid-name
        """Return an authorisation header for MyTardis"""
        r.headers["Authorization"] = f"ApiKey {self.username}:{self.api_key}"
        return r


class BadGateWayException(RequestException):
    """A specific exception for 502 errors to trigger backoff retries.

    502 Bad Gateway triggers retries, since the proxy web server (eg Nginx
    or Apache) in front of MyTardis could be temporarily restarting
    """

    # Included for clarity even though it is unnecessary
    def __init__(
        self, response: Response
    ) -> None:  # pylint: disable=useless-super-delegation
        super().__init__(response)


class ApiProxy(BaseModel):
    """MyTardis proxy configuration.

    Pydantic model for holding MyTardis proxy configuration.

    Attributes:
        http : Optional[HttpUrl] (default: None)
            http proxy address
        https : Optional[HttpUrl] (default: None)
            https proxy address
    """

    http: Optional[str] = None
    https: Optional[str] = None


class MyTardisRestAgent:  # pylint: disable=R0903, R0913
    """Class for handling requests to MyTardis API

    Raises:
        BadGateWayException: raised if connection to MT proxy fails

    """

    connection_hostname: str
    _api_stub: str = "/api/v1/"

    auth: AuthConfig = AuthConfig(username="", api_key="")
    user_agent_name: str = __name__
    user_agent_url: str = "https://github.com/UoA-eResearch/ro_crate_mt_ingestions"

    def __init__(
        self,
        auth_config: Optional[AuthConfig],
        connection_hostname: Optional[str],
        connection_proxies: Optional[ApiProxy],
        verify_certificate: bool,
    ) -> None:
        self.hostname = (
            connection_hostname if connection_hostname else CONNECTION__HOSTNAME
        )
        if not auth_config:
            logger.warning(
                """could not find API authentication details (name or key)
                 requests that require authentication will fail"""
            )
        else:
            self.auth = auth_config
        self.proxies = connection_proxies.model_dump() if connection_proxies else None

        self.verify_certificate = verify_certificate
        self.api_template = urljoin(self.hostname, self._api_stub)
        self.user_agent = f"{self.user_agent_name}/2.0 ({self.user_agent_url})"
        self._session = requests.Session()

    @backoff.on_exception(backoff.expo, BadGateWayException, max_tries=8)
    def mytardis_api_request(  # pylint: disable=R0903, R0913
        self,
        method: str,  # REST api method
        url: str,
        data: Optional[str] = None,
        params: Optional[Dict[str, str]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Response:
        """Function to handle the REST API calls

        Takes a REST method and url and prepares a requests request. Once the request has been
        made, the function returns the response or raises an error if the response was not process
        properly.

        502 Bad Gateway error trigger retries, since the proxy web server (eg Nginx or Apache) in
        front of MyTardis could be temporarily restarting. This uses the @backoff decorator and is
        limited to 8 retries (can be changed in the decorator.

        Args:
            method: The REST API method, POST, GET etc.
            url: The API URL for the call requested
            data: A JSON string containing data for generating an object via POST/PUT
            params: A JSON string of parameters to be passed in the URL
            extra_headers: Extra headers (META) to be passed to the API call

        Returns:
            A requests.Response object

        Raises:
            RequestException: An error raised when the request was not able to be completed due to
                502 Bad Gateway error
            HTTPError: An error raised when the request fails for other reasons via the
                requests.Response.raise_for_status function.
        """
        if method == "POST" and url[-1] != "/":
            url = f"{url}/"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": self.user_agent,
        }
        if extra_headers:
            headers = {**headers, **extra_headers}
        response = self._session.request(
            method,
            url,
            data=data,
            params=params,
            headers=headers,
            auth=self.auth,
            verify=self.verify_certificate,
            proxies=self.proxies,
            timeout=5,
        )

        if response.status_code == 502:
            raise BadGateWayException(response)
        response.raise_for_status()

        return response

    def create_person_object(self, upi: str) -> Person:
        """Look up a UPI and create a Person entry from the results

        Args:
            upi (str): The UPI for the person

        Returns:
            Person: A Person object created from the results of the UPI lookup

        Raises:
            ValueError: If the UPI can't be found
        """
        users_stub = "user/?username="
        name = upi
        email = ""
        try:
            response = self.mytardis_api_request(
                "GET", self.api_template + users_stub + upi
            )
            if response.status_code == 200:
                if response_data := response.json().get("objects"):
                    name = (
                        response_data[0].get("first_name")
                        if response_data[0].get("first_name")
                        else name
                    )
                    name += (
                        " " + response_data[0].get("last_name")
                        if response_data[0].get("last_name")
                        else ""
                    )
                    email = (
                        response_data[0].get("email")
                        if response_data[0].get("email")
                        else email
                    )
        except RequestException as e:
            logger.error("bad API response getting person data for %s: %s", upi, e)

        return Person(name=name, email=email, affiliation=UOA, identifiers=[upi])

    def no_auth_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        """Make a request to the MyTardis API without requiring authentication.
        Mainly used for GET requests to non-sensitive data that don't requaire AUTH

        Args:
            method (str): the REST API method
            url (str): the hostname URL with API request
            params (Optional[Dict[str, str]], optional): additional parameters

        Returns:
            A requests.Response object

        Raises:
            RequestException: An error raised when the request was not able to be completed due to
                502 Bad Gateway error
            HTTPError: An error raised when the request fails for other reasons via the
                requests.Response.raise_for_status function.
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": self.user_agent,
        }
        response = requests.request(
            method,
            url,
            params=params,
            headers=headers,
            verify=self.verify_certificate,
            proxies=self.proxies,
            timeout=5,
        )

        if response.status_code == 502:
            raise BadGateWayException(response)
        response.raise_for_status()

        return response
