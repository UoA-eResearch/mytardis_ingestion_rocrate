import requests

from pydantic import BaseModel
from typing import Dict, Any, Optional
from pydantic_settings import SettingsConfigDict, BaseSettings
from pydantic import ValidationError
import logging

from src.ingestion_targets.print_lab_genomics.print_crate_dataclasses import (
    MedicalCondition
)

logger = logging.getLogger(__name__)

class ICD11_object():
    pass

class ICD11_Auth(BaseSettings):
    ICD11client_id: Optional[str] = None
    ICD11client_secret: Optional[str] = None
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

class ICD_11_Api_Agent():
    token: str
    auth_details: ICD11_Auth

    def __init__(self):
        self.linearizationname = "mms"
        self.releaseId = "2024-01"
        self.auth_details = ICD11_Auth()
        self.request_token()           
        self.headers = {'Authorization':  'Bearer '+str(self.token), 
           'Accept': 'application/json', 
           'Accept-Language': 'en',
	   'API-Version': 'v2'}


    def request_token(self) -> None:
        # get the OAUTH2 token
        token_endpoint = 'https://icdaccessmanagement.who.int/connect/token'
        scope = 'icdapi_access'
        grant_type = 'client_credentials'
        payload = {'client_id': self.auth_details.ICD11client_id, 
	   	   'client_secret': self.auth_details.ICD11client_secret, 
           'scope': scope, 
           'grant_type': grant_type}
        r = requests.post(token_endpoint, data=payload, verify=True)
        if r.status_code == 200:
            self.token = r.json().get('access_token')
        else:
            logger.error("Bad Token response from ICD-11, data will not be updated via API.\n %r", r)
            self.token = None
        return self.token

               
    def request_ICD11_data(self, code) -> Dict[str,Any]:
        if self.token is None:
            return None
        code_request =  f" https://id.who.int/icd/release/11/{self.releaseId}/{self.linearizationname}/codeinfo/{code}?flexiblemode=false&convertToTerminalCodes=false"
        #request based on code
        try:       
            r = requests.get(code_request, headers=self.headers, verify=True)
            #request based on entity id
            if r.status_code == 200:
                r = requests.get(r.json()["stemId"], headers=self.headers, verify=True)
                return r.json()
            else:
                logger.error("bad response for code:%s , response: %s", code,r)
                return None
        except requests.RequestException as e:
            logger.error(
                "bad ICD11 API response", r, e
            )
          
# make request
    def update_medial_entity_from_ICD11(self, medical_condition: MedicalCondition) -> MedicalCondition:
        try:
            ICD11_data = self.request_ICD11_data(medical_condition.code)
            if ICD11_data is None:
                logger.warn("Information could not be retreived from ICD-11 API for code %s. Data will not be updated", medical_condition.code)
                return medical_condition
            
            object.__setattr__(medical_condition, "identifier", ICD11_data["@id"])
            medical_condition.code_text = ICD11_data["title"]["@value"]
            medical_condition.code_source = ICD11_data["source"]
        except requests.RequestException as e:
            logger.warn("ICD-11 API error: %s", e)
            return medical_condition
        return medical_condition


agent = ICD_11_Api_Agent()
print(agent.request_ICD11_data("2C10.1"))

