
#pylint: disable = invalid-name
"""Classes for requesting data from the ICD11 API
"""
import logging
from typing import Any, Dict, Optional

import requests
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.ingestion_targets.print_lab_genomics.print_crate_dataclasses import (
    MedicalCondition,
)

logger = logging.getLogger(__name__)


class ICD11_Auth(BaseSettings):
    """Authorization settings for the ICD-11 API
    """
    ICD11client_id: Optional[str] = None
    ICD11client_secret: Optional[str] = None
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


class ICD_11_Api_Agent:
    """Agent for requesting data from the ICD-11 API
    """
    token: str
    auth_details: ICD11_Auth

    def __init__(self) -> None:
        self.default_linearizationname = "mms"
        self.releaseId = "2024-01"
        self.auth_details = ICD11_Auth()
        self.request_token()
        self.headers = {
            "Authorization": "Bearer " + str(self.token),
            "Accept": "application/json",
            "Accept-Language": "en",
            "API-Version": "v2",
        }

    def request_token(self) -> None:
        """Request the OAUTH2 token from the ICD-11 and store it on this agent
        """
        # get the OAUTH2 token
        token_endpoint = "https://icdaccessmanagement.who.int/connect/token"
        scope = "icdapi_access"
        grant_type = "client_credentials"
        payload = {
            "client_id": self.auth_details.ICD11client_id,
            "client_secret": self.auth_details.ICD11client_secret,
            "scope": scope,
            "grant_type": grant_type,
        }
        r = requests.post(token_endpoint, data=payload, verify=True, timeout=60)
        if r.status_code == 200:
            self.token = r.json().get("access_token")
        else:
            logger.error(
                "Bad Token response from ICD-11, data will not be updated via API.\n %r",
                r,
            )
            self.token = ""

    def request_ICD11_data(
        self,
        code: str,
        linearizationname:str) -> Any:
        """
        Request data from the ICD-11 based on the ICD-11 code in a specific linearization
        """

        if self.token is "":
            return None
        code_request = f"https://id.who.int/icd/release/11/{self.releaseId}/{linearizationname}/codeinfo/{code}?flexiblemode=false&convertToTerminalCodes=false"""#pylint: disable = line-too-long
        # request based on code
        try:
            r = requests.get(code_request, headers=self.headers, verify=True, timeout=25)
            # request based on entity id
            if r.status_code == 200:
                r = requests.get(r.json()["stemId"], headers=self.headers, verify=True, timeout=5)
                return r.json()

            logger.error("bad response for code:%s , response: %s", code, r)
            return None
        except requests.RequestException as e:
            logger.error("bad ICD11 API response %s", e)
            return None
    # make request
    def update_medial_entity_from_ICD11(
        self, medical_condition: MedicalCondition
    ) -> MedicalCondition:
        """Update a medical condition object from the ICD-11

        Args:
            medical_condition (MedicalCondition): a medical condition to be writting to an RO-Crate

        Returns:
            MedicalCondition: the Medical condition with any relevant ICD-11 data
        """
        try:
            ICD11_data = self.request_ICD11_data(
                medical_condition.code,
                self.default_linearizationname)
            if ICD11_data is None:
                logger.warning(
                    """Information could not be retreived from ICD-11 API for code %s. 
                    Data will not be updated""",
                    medical_condition.code,
                )
                return medical_condition

            object.__setattr__(medical_condition, "identifier", ICD11_data["@id"])
            medical_condition.code_text = ICD11_data["title"]["@value"]
            medical_condition.code_source = ICD11_data["source"]
        except requests.RequestException as e:
            logger.warning("ICD-11 API error: %s", e)
            return medical_condition
        return medical_condition
