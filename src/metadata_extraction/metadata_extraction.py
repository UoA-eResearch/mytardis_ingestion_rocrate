"""Metadata conversion and generation"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    MTMetadata,
    MyTardisContextObject,
)
from requests.exceptions import RequestException

from src.cli.mytardisconfig import SchemaConfig
from src.mt_api.apiconfigs import MyTardisRestAgent
from src.mt_api.mt_consts import MtObject

MT_METADATA_TYPE = {
    1: "NUMERIC",
    2: "STRING",
    3: "URL",
    4: "LINK",
    5: "FILENAME",
    6: "DATETIME",
    7: "LONGSTRING",
    8: "JSON",
    "default": "STRING",
}
logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class MetadataSchema:
    """A class for holding bundled information about a metadata schema"""

    schema: Dict[str, Dict[str, Any]]
    url: str
    mt_type: Optional[MtObject] = None


def get_metadata_type(type_enum: int) -> str:
    """Get the data type matching the MyTardis API schema
    defaults to string if the value can't be found

    Args:
        type_enum (int): get the integer for the lookup table of Metadata types

    Returns:
        str: the metadata type as a string
    """
    if MT_METADATA_TYPE.get(type_enum) is not None:
        return MT_METADATA_TYPE[type_enum]
    return MT_METADATA_TYPE["default"]


class MetadataHanlder:
    """Class for loading metadata schemas and creating metadata RO-Crate objects

    api_agenent MyTardisRestAgent: the api_agnet for making requests for the schemas
    """

    api_agent: MyTardisRestAgent
    metadata_schemas: Dict[MtObject, Dict[Any, Any]]
    metadata_collected: Dict[str, MTMetadata] = {}
    pubkey_fingerprints: Optional[List[str]] = None

    def __init__(
        self,
        api_agent: MyTardisRestAgent,
        schema_namespaces: Dict[MtObject, str],
        pubkey_fingerprints: Optional[List[str]],
    ):
        self.api_agent = api_agent
        self.schema_namespaces = schema_namespaces
        self.request_metadata_dicts(schema_namespaces)
        self.pubkey_fingerprints = pubkey_fingerprints

    def request_metadata_schema(self, schema_namespace: str) -> Dict[Any, Any]:
        """Requests a metadata schema from the MyTardis API based on namespace

        Args:
            schema_namespace (str): the namespace of hte requested schema

        Returns:
            Dict[Any, Any]: the metadata schema as a dictionary keyed on its' "full name"
        """
        schema_stub = "schema/?namespace="
        metadata_params = []
        try:
            response = self.api_agent.no_auth_request(
                "GET", self.api_agent.api_template + schema_stub + schema_namespace
            )
            metadata_response = [
                response_obj.get("parameter_names")
                for response_obj in response.json().get("objects")
            ]
            metadata_params = [
                metatdata_parameters
                for params in metadata_response
                for metatdata_parameters in params
            ]
        except RequestException as e:
            logger.error(
                "bad API response getting Metadata schema %s: %s \n NO METADATA WILL BE READ",
                schema_namespace,
                e,
            )

        return {
            schema_object.get("name"): schema_object
            for schema_object in metadata_params
        }

    def request_metadata_dicts(
        self, schema_namespaces: Dict[MtObject, str]
    ) -> Dict[MtObject, Dict[Any, Any]]:
        """Load a set of schemas via the MyTardis API based on namespaces

        Args:
            schema_namespaces (Dict[MtObject, str]):
             A dictionary of Metadata namespaces corresponding to MyTardis objects

        Returns:
            Dict[MtObject, list[Any]]: metadata schemas for creating RO-Crate objects
        """
        metadata_schemas = {}
        for mt_object, namespace in schema_namespaces.items():
            metadata_schemas[mt_object] = self.request_metadata_schema(namespace)
        self.metadata_schemas = metadata_schemas
        return metadata_schemas

    def get_mtobj_schema(self, schema_name: MtObject) -> Dict[str, Dict[str, Any]]:
        """Convert a metadata schema into a lookup dictonary using the full name of the metadata

        Args:
            schema_name (MtObject): which mytardis object schema based on the schema dictonary

        Returns:
             Dict[str, Dict[str, Any]]: a dictionary of metadata elements keyed by their full name
        """
        all_schema_objects = self.metadata_schemas.get(schema_name)
        if not all_schema_objects:
            return {}
        return all_schema_objects

    def create_metadata_from_schema(
        self,
        input_metadata: Dict[str, Any],
        mt_object: MtObject,
        collect_all: bool,
        parent: MyTardisContextObject,
    ) -> Dict[str, MTMetadata]:
        """Create a new metadata object from schemas

        Args:
            input_metadata (Dict[str, Any]): input metadata to be read in
            mt_object (MtObject): what kind of mytardis object is this
            collect_all (bool): collect all metadata from this object
            parent (MyTardisContextObject): what parent should we associate this metadata with

        Returns:
            Dict[str, MTMetadata]: all the metadata collected
        """
        metadata_schema = MetadataSchema(
            schema=self.get_mtobj_schema(mt_object),
            url=self.schema_namespaces.get(mt_object) or "",
            mt_type=mt_object,
        )
        metadata_dict = create_metadata_objects(
            input_metadata=input_metadata,
            metadata_schema=metadata_schema,
            collect_all=collect_all,
            parent=parent,
            pubkey_fingerprints=self.pubkey_fingerprints,
        )
        self.metadata_collected.update(metadata_dict)
        return metadata_dict


def create_metadata_objects(
    input_metadata: Dict[str, Any],
    metadata_schema: MetadataSchema,
    collect_all: bool = False,
    parent: Optional[MyTardisContextObject] = None,
    pubkey_fingerprints: Optional[List[str]] = None,
) -> Dict[str, MTMetadata]:
    """Create RO-Crate metadata objects from an input dictionary and a schema for lookup

    Args:
        input_metadata (Dict[str, Any]): the metadata from an input file
        metadata_schema (Dict[str, Dict[str, Any]]): the schema for getting info from MyTardis

    Returns:
        Dict[str, MTMetadata]: RO-Crate metadata objects for stroing MyTardis metadata
    """
    metadata_dict: Dict[str, MTMetadata] = {}
    for meta_key, meta_value in input_metadata.items():
        if (meta_key and meta_value) and (
            collect_all or meta_key in metadata_schema.schema
        ):
            # if we have metadata and the info to store it
            metadata_type = 2
            metadata_sensitive = False
            if metadata_object := metadata_schema.schema.get(meta_key):
                metadata_type = metadata_object.get("data_type")  # type: ignore
                metadata_sensitive = bool(metadata_object.get("sensitive"))
            metadata_dict[meta_key] = MTMetadata(
                name=meta_key,
                value=str(meta_value),  # if metadata_type == 2 else meta_value,
                mt_type=get_metadata_type(int(metadata_type)),
                mt_schema=metadata_schema.url,
                sensitive=metadata_sensitive,
                parent=parent if parent else None,
                pubkey_fingerprints=pubkey_fingerprints,
            )
    return metadata_dict


def load_optional_schemas(
    namespaces: Dict[MtObject, str], schemas: SchemaConfig | None
) -> Dict[MtObject, str]:
    """Update some set of default namespaces with those loaded from config. If they are present.

    Args:
        namespaces (Dict[MtObject, str]): default schema namespaces
        schemas (SchemaConfig): namespaces loaded via a schemaConfig

    Returns:
        Dict[MtObject, str]: default schema namespaces now updated with those from config
    """
    if not schemas:
        return namespaces
    namespaces[MtObject.PROJECT] = (
        schemas.project if schemas.project else namespaces[MtObject.PROJECT]
    )
    namespaces[MtObject.EXPERIMENT] = (
        schemas.experiment if schemas.experiment else namespaces[MtObject.EXPERIMENT]
    )
    namespaces[MtObject.DATASET] = (
        schemas.dataset if schemas.dataset else namespaces[MtObject.DATASET]
    )
    namespaces[MtObject.DATAFILE] = (
        schemas.datafile if schemas.datafile else namespaces[MtObject.DATAFILE]
    )
    return namespaces
