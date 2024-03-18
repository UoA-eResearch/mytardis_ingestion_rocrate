"""Metadata conversion and generation"""

from typing import Any, Dict

from src.mt_api.apiconfigs import MyTardisRestAgent
from src.mt_api.mt_consts import MtObject
from src.rocrate_dataclasses.rocrate_dataclasses import MTMetadata

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
    metadata_schemas: Dict[MtObject, list[Any]]

    def __init__(
        self, api_agent: MyTardisRestAgent, schema_namespaces: Dict[MtObject, str]
    ):
        self.api_agent = api_agent
        self.request_metadata_dicts(schema_namespaces)

    def request_metadata_dicts(
        self, schema_namespaces: Dict[MtObject, str]
    ) -> Dict[MtObject, list[Any]]:
        """Load a set of schemas via the MyTardis API based on namespaces

        Args:
            schema_namespaces (Dict[MtObject, str]):
             A dictionary of Metadata namespaces corresponding to MyTardis objects

        Returns:
            Dict[MtObject, list[Any]]: metadata schemas for creating RO-Crate objects
        """
        schema_stub = "schema/?namespace="
        metadata_schemas = {}

        def request_metadata_schema(schema_namespace: str) -> list[Any]:
            response = self.api_agent.mytardis_api_request(
                "GET", self.api_agent.api_template + schema_stub + schema_namespace
            )
            metadata_response = [
                response_obj.get("parameter_names")
                for response_obj in response.json().get("objects")
            ]
            return [
                metatdata_parameters
                for params in metadata_response
                for metatdata_parameters in params
            ]

        for mt_object, namespace in schema_namespaces.items():
            metadata_schemas[mt_object] = request_metadata_schema(namespace)
        self.metadata_schemas = metadata_schemas
        return metadata_schemas

    def get_metdata_lookup_dict(
        self, schema_name: MtObject
    ) -> Dict[str, Dict[str, Any]]:
        """Convert a metadata schema into a lookup dictonary using the full name of the metadata

        Args:
            schema_name (MtObject): which mytardis object schema based on the schema dictonary

        Returns:
             Dict[str, Dict[str, Any]]: a dictionary of metadata elements keyed by their full name
        """
        all_schema_objects = self.metadata_schemas.get(schema_name)
        if not all_schema_objects:
            return {}
        return {
            schema_object.get("full_name"): schema_object
            for schema_object in all_schema_objects
        }

    def create_metadata_objects(
        self, input_metadata: Dict[str, Any], metadata_schema: Dict[str, Dict[str, Any]]
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
            if meta_key in metadata_schema:
                metadata_type = 2
                metadata_sensitive = False
                if metadata_object := metadata_schema.get(meta_key):
                    metadata_type = metadata_object.get("data_type")  # type: ignore
                    metadata_sensitive = bool(metadata_object.get("sensitive"))
                metadata_dict[meta_key] = MTMetadata(
                    ro_crate_id=meta_key,
                    name=meta_key,
                    value=meta_value,
                    mt_type=get_metadata_type(int(metadata_type)),
                    sensitive=metadata_sensitive is True,
                )
        return metadata_dict
