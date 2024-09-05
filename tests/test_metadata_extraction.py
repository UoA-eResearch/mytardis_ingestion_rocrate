#pylint: disable=too-many-arguments
"""Tests For metadata construction
"""
from typing import Any, Dict

import pandas as pd
import responses
from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import (
    MTMetadata,
    Project,
)

from src.metadata_extraction.metadata_extraction import (
    MetadataHanlder,
    MetadataSchema,
    create_metadata_objects,
)
from src.mt_api.api_consts import CONNECTION__HOSTNAME
from src.mt_api.apiconfigs import AuthConfig, MyTardisRestAgent
from src.mt_api.mt_consts import MtObject


@responses.activate
def test_retreive_schema(
    auth: AuthConfig,
    test_metadata_response: Dict[str, Any],
    test_schema_namespace: str,
    test_schema: Dict[str, Any],
) -> None:
    """Test retreiving and constructing schemas from the myTardis API

    Args:
        auth (AuthConfig): test authentication config
        test_metadata_response (Dict[str, Any]): test api response for a metadata schema
        test_schema_namespace (str): test namespace that corresponds to the test metadata
        test_schema (Dict[str, Any]): the schema retreived
    """
    mt_rest_agent = MyTardisRestAgent(auth, CONNECTION__HOSTNAME, None, False)
    schema_stub = "schema/?namespace="
    responses.add(
        responses.GET,
        mt_rest_agent.api_template + schema_stub + test_schema_namespace,
        status=200,
        json=test_metadata_response,
    )
    schema_namespaces = {MtObject.PROJECT: test_schema_namespace}
    handler = MetadataHanlder(
        api_agent=mt_rest_agent, schema_namespaces=schema_namespaces
    )
    schemas = handler.request_metadata_dicts(schema_namespaces=schema_namespaces)
    assert schemas[MtObject.PROJECT] == test_schema


def test_create_metadata_objects(
    faked_projects_row: pd.Series,
    test_parent_project: Project,
    test_schema_object: MetadataSchema,
    test_output_metadata: MTMetadata,
    test_output_sensitive_metadata: MTMetadata,
) -> None:
    """Test creating metadata objects based on schemas and faked data

    Args:
        faked_projects_row (pd.Series): one project's worth of faked data
        test_parent_project (Project): a parent to all metadata
        test_schema_object (MetadataSchema): the metadata schema object for constructing metadata
        test_output_metadata (MTMetadata): expected output metadata
        test_output_sensitive_metadata (MTMetadata): another expected output metadata
    """
    row = faked_projects_row
    metadata = create_metadata_objects(
        input_metadata=row,
        metadata_schema=test_schema_object,
        collect_all=False,
        parent=test_parent_project,
    )
    assert metadata["Project code"] == test_output_metadata
    assert metadata["Patient Consent Designation"] == test_output_sensitive_metadata


@responses.activate
def test_create_metadata_from_schema(
    auth: AuthConfig,
    test_parent_project: Project,
    test_metadata_response: Dict[str, Any],
    test_schema_namespace: str,
    faked_projects_row: pd.Series,
    test_output_metadata: MTMetadata,
    test_output_sensitive_metadata: MTMetadata,
) -> None:
    """Test the full metadata construction process

    Args:
        auth (AuthConfig): authentication config
        test_parent_project (Project): the metadata parent (so IDS are consistent)
        test_metadata_response (Dict[str, Any]): response from MT_api for metadata schema
        test_schema_namespace (str): namespace for test metadata
        faked_projects_row (pd.Series): one row of faked projects data
        test_output_metadata (MTMetadata): expected output metadata
        test_output_sensitive_metadata (MTMetadata): another expected output metadata
    """

    mt_rest_agent = MyTardisRestAgent(auth, CONNECTION__HOSTNAME, None, False)
    schema_stub = "schema/?namespace="
    responses.add(
        responses.GET,
        mt_rest_agent.api_template + schema_stub + test_schema_namespace,
        status=200,
        json=test_metadata_response,
    )
    schema_namespaces = {MtObject.PROJECT: test_schema_namespace}
    handler = MetadataHanlder(
        api_agent=mt_rest_agent, schema_namespaces=schema_namespaces
    )
    metadata = handler.create_metadata_from_schema(
        input_metadata=faked_projects_row,
        mt_object=MtObject.PROJECT,
        collect_all=False,
        parent=test_parent_project,
    )
    assert metadata["Project code"] == test_output_metadata
    assert metadata["Patient Consent Designation"] == test_output_sensitive_metadata


# Test API requests!

# Test ICD11 API
# test ICD11 request
# test ICD11 token
# test ICD 11 replace medical condition
