"""
A stripped down version of MyTardis configuration module (config.py)
for reading config files provided for MyTardis ingestions
"""

from typing import Any, Optional
from urllib.parse import urljoin

from pydantic import BaseModel, PrivateAttr
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.mt_api.apiconfigs import AuthConfig


class SchemaConfig(BaseModel):
    """MyTardis default schema configuration.

    Pydantic model for MyTardis default schema configuration.

    Attributes:
        project : Optional[AnyUrl] (default: None)
            default project schema
        experiment : Optional[AnyUrl] (default: None)
            default experiment schema
        dataset : Optional[AnyUrl] (default: None)
            default dataset schema
        datafile : Optional[AnyUrl] (default: None)
            default datafile schema
    """

    project: Optional[str] = None
    experiment: Optional[str] = None
    dataset: Optional[str] = None
    datafile: Optional[str] = None


class ProxyConfig(BaseModel):
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


class ConnectionConfig(
    BaseModel
):  # pylint: disable =too-few-public-methods # type: ignore
    """MyTardis connection configuration.

    Pydantic model for MyTardis connection configuration.

    Attributes:
        hostname : HttpUrl
            MyTardis instance base URL
        verify_certificate : bool (default: True)
            Checks the validity of the host certificate if `True`
        proxy : ProxyConfig (default: None)

    Properties:
        api_template : str
            Returns the stub of the MyTardis API route
    """

    hostname: str
    verify_certificate: bool = True
    proxy: Optional[ProxyConfig] = None
    _api_stub: str = PrivateAttr("/api/v1/")

    @property
    def api_template(self) -> Any:
        """Appends the API stub to the configured hostname and returns it"""
        return urljoin(self.hostname, self._api_stub)


class PubKeyConfig(BaseModel):
    """Class for holding public keys and names loaded from env configs"""

    key: Optional[str] = None
    name: Optional[str] = None


class MyTardisEnvConfig(BaseSettings):
    """Reads a MyTardis config file to fill out API Keys and Default namespaces

    Attributes:

        auth : AuthConfig
            instance of Pydantic auth model
        connection : ConnectionConfig
            instance of Pydantic connection model
        default_schema : SchemaConfig
            instance of Pydantic schema model, used to fill namespaces
    """

    auth: AuthConfig
    connection: ConnectionConfig
    default_schema: SchemaConfig
    mytardis_pubkey: PubKeyConfig
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )
