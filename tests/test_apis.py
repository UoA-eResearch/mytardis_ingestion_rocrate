"""Test the MyTardis and ICD11 APIs"""

# pylint: disable=redefined-outer-name,invalid-name,protected-access
from typing import Any, Dict

import mock
import pytest
import responses
from mock import MagicMock
from requests import HTTPError, RequestException
from requests.models import MissingSchema

from src.ingestion_targets.print_lab_genomics.ICD11_API_agent import ICD11ApiAgent
from src.ingestion_targets.print_lab_genomics.print_crate_dataclasses import (
    MedicalCondition,
)
from src.mt_api.api_consts import CONNECTION__HOSTNAME
from src.mt_api.apiconfigs import AuthConfig, MyTardisRestAgent


@responses.activate
def test_mytardis_rest_agent_get(
    test_metadata_response: Dict[str, Any], auth: AuthConfig, test_schema_namespace: str
) -> None:
    """Test a basic get request from the MyTardis API agent

    Args:
        test_metadata_response (Dict[str, Any]): a test response expected from the agent
        auth (AuthConfig): a test authentication config
        test_schema_namespace (str): a test namespace for schemas to construct the request
    """
    mt_rest_agent = MyTardisRestAgent(auth, CONNECTION__HOSTNAME, None, False)
    schema_stub = "schema/?namespace="
    url = mt_rest_agent.api_template + schema_stub + test_schema_namespace
    responses.add(
        responses.GET,
        url,
        status=200,
        json=test_metadata_response,
    )

    response = mt_rest_agent.mytardis_api_request("GET", url)
    assert response.ok is True

    # test bad responses
    responses.add(
        responses.GET,
        url,
        status=404,
        json=test_metadata_response,
    )
    with pytest.raises(RequestException):
        response = mt_rest_agent.mytardis_api_request("GET", url)

    responses.add(
        responses.GET,
        url,
        status=500,
        json=test_metadata_response,
    )
    with pytest.raises(HTTPError):
        response = mt_rest_agent.mytardis_api_request("GET", url)


@responses.activate
def test_mytardis_rest_agent_no_auth(
    test_metadata_response: Dict[str, Any], test_schema_namespace: str
) -> None:
    """Test a request that does not need authorization

    Args:
        test_metadata_response (Dict[str, Any]): a test response expected from the agent
        auth (AuthConfig): a test authentication config
        test_schema_namespace (str): a test namespace for schemas to construct the request
    """
    do_not_use_auth = MagicMock()
    mt_rest_agent = MyTardisRestAgent(
        do_not_use_auth, CONNECTION__HOSTNAME, None, False
    )
    schema_stub = "schema/?namespace="
    url = mt_rest_agent.api_template + schema_stub + test_schema_namespace
    responses.add(
        responses.GET,
        url,
        status=200,
        json=test_metadata_response,
    )

    _ = mt_rest_agent.no_auth_request("GET", url)
    do_not_use_auth.assert_not_called()


@responses.activate
def test_mytardis_rest_agent_get_fail() -> None:
    """test expected fail conditions on a MyTardis request"""
    mt_rest_agent = MyTardisRestAgent(
        AuthConfig(username="", api_key=""), CONNECTION__HOSTNAME, None, False
    )

    responses.add(
        responses.GET,
        "bad_url.com",
        status=400,
        json=None,
    )
    with pytest.raises(MissingSchema):
        mt_rest_agent.mytardis_api_request("GET", "bad_url.com")

    responses.add(
        responses.GET,
        "https://bad_url.com",
        status=400,
        json=None,
    )
    with pytest.raises(HTTPError):
        mt_rest_agent.mytardis_api_request("GET", "https://bad_url.com")


@responses.activate
def test_icd_11_api_get_token() -> None:
    """Test getting a token for the ICD11 agent"""
    ICD11Auth = MagicMock()
    ICD11Auth.ICD11client_id.return_value = "test_client_id"
    ICD11Auth.ICD11client_secret.return_value = "test_client_secret"
    token_url = "https://icdaccessmanagement.who.int/connect/token"
    # check token acquired without error
    responses.add(
        method=responses.POST,
        url=token_url,
        status=200,
        json={"access_token": "token_response"},
    )
    # check token can be updated later if needed
    idc11_agent = ICD11ApiAgent()
    idc11_agent.token = "token_response"
    responses.add(
        method=responses.POST,
        url=token_url,
        status=200,
        json={"access_token": "token_response_2"},
    )
    idc11_agent._request_token()
    assert idc11_agent.token == "token_response_2"


@responses.activate
def test_request_idc11_data(
    test_icd_11_code: str, test_icd11_condition: Dict[str, Any]
) -> None:
    """Test requesting ICD11 data
    Test that a first request to translate code into UUID leads to a second
    and that if the first request fails then the second  fails gracefully

    Args:
        test_icd_11_code (str): the code of the ICD11 condition
        test_icd11_condition (Dict[str, Any]): the condition information retrieved from ICD11
    """
    ICD11Auth = MagicMock()
    ICD11Auth.ICD11client_id.return_value = "test_client_id"
    ICD11Auth.ICD11client_secret.return_value = "test_client_secret"
    with mock.patch.object(ICD11ApiAgent, "_request_token") as _request_token:

        idc11_agent = ICD11ApiAgent()
        idc11_agent.token = "Return_token"
        assert idc11_agent.token == "Return_token"

        code_request = f"https://id.who.int/icd/release/11/{idc11_agent.releaseId}/{idc11_agent.default_linearizationname}/codeinfo/{test_icd_11_code}?flexiblemode=false&convertToTerminalCodes=false"  # pylint: disable = line-too-long
        resolved_code_id = "https://id.who.int/resolved_code_uuid"
        responses.add(
            method=responses.GET,
            headers=idc11_agent.headers,
            url=code_request,
            status=200,
            json={"stemId": resolved_code_id},
        )
        # two requests are made to resolve ICD11 uuid from code
        responses.add(
            method=responses.GET,
            headers=idc11_agent.headers,
            url=resolved_code_id,
            status=200,
            json=test_icd11_condition,
        )
        icd_11_data = idc11_agent.request_ICD11_data(
            test_icd_11_code, idc11_agent.default_linearizationname
        )
        assert icd_11_data == test_icd11_condition
        # test failing gracefully
        responses.add(
            method=responses.GET,
            headers=idc11_agent.headers,
            url=code_request,
            status=400,
            json={},
        )
        icd_11_data = idc11_agent.request_ICD11_data(
            test_icd_11_code, idc11_agent.default_linearizationname
        )
        assert icd_11_data is None


def test_update_medical_entity(
    test_icd11_condition: Dict[str, Any],
    test_medical_condition: MedicalCondition,
    test_updated_medical_condition: MedicalCondition,
) -> None:
    """Test updating a medical entity using information from the ICD-11 API

    Args:
        test_icd11_condition (Dict[str, Any]):  the condition information retrieved from ICD11
        test_medical_condition (MedicalCondition): the medical condition pre-update
        test_updated_medical_condition (MedicalCondition): the medical condition post-update
    """
    ICD11Auth = MagicMock()
    ICD11Auth.ICD11client_id.return_value = "test_client_id"
    ICD11Auth.ICD11client_secret.return_value = "test_client_secret"
    ICD11ApiAgent.token = "Return_token"

    with mock.patch.object(ICD11ApiAgent, "_request_token") as _request_token:
        idc11_agent = ICD11ApiAgent()

        with mock.patch.object(idc11_agent, "request_ICD11_data") as icd_return:
            icd_return.return_value = test_icd11_condition
            updated_medical_condition = idc11_agent.update_medial_entity_from_ICD11(
                test_medical_condition
            )
            assert updated_medical_condition == test_updated_medical_condition
