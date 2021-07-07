# Functions to read/write from the project database as necessary (once it is working)

import logging
import requests
from requests.auth import AuthBase
from decouple import Config, RepositoryEnv
from urllib.parse import urljoin, quote
import backoff
import json
import gnupg
import os

logger = logging.getLogger(__name__)

def dict_to_json(dictionary):
    """
    Serialize a dictionary to JSON, correctly handling datetime.datetime
    objects (to ISO 8601 dates, as strings).

    Input:
    =================================
    dictionary: Dictionary to serialise

    Returns:
    =================================
    JSON string
    """
    import json
    import datetime
    if not isinstance(dictionary, dict):
        raise TypeError("Must be a dictionary")

    def date_handler(obj): return (
        obj.isoformat()
        if isinstance(obj, datetime.date)
        or isinstance(obj, datetime.datetime)
        else None
    )
    return json.dumps(dictionary, default=date_handler)

class ProjectDBAuth(AuthBase):

    def __init__(self,
                 api_key):
        self.api_key = api_key

    def __call__(self,
                 request):
        request.headers['apikey'] = f'{self.api_key}'
        return request

class ProjectDBFactory():

    def __init__(self):
        self.project_db_url = os.environ.get('PROJECT_DB_URL')
        self.api_key = os.environ.get('PROJECT_DB_API')
        self.auth = ProjectDBAuth(self.api_key)
        self.proxies = None
        self.verify_certificate = True
        '''self.proxies = {"http": global_config('PROXY_HTTP',
                                              default=None),
                        "https": global_config('PROXY_HTTPS',
                                               default=None)}
        if self.proxies['http'] == None and self.proxies['https'] == None:
            self.proxies = None
        self.verify_certificate = global_config('PROJECT_DB_VERIFY_CERT',
                                                default=True,
                                                cast=bool)'''

    @backoff.on_exception(backoff.expo,
                          requests.exceptions.RequestException,
                          max_tries=8)
    def __rest_api_call(self,
                        method,
                        url, 
                        data=None,
                        params=None,
                        extra_headers=None):
        '''Function to handle the REST API calls
        
        Inputs:
        =================================
        method: The REST API method, POST, GET etc.
        action: The object type to call REST API on, e.g. experiment, dataset
        data: A JSON string containing data for generating an object via POST/PUT
        params: A JSON string of parameters to be passed in the URL
        extra_headers: Extra headers (META) to be passed to the API call
        api_url_template: Over-ride for the default API URL
        
        Returns:
        =================================
        A Python Requests library repsonse object
        '''
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json'}
        if extra_headers:
            headers = {**headers, **extra_headers}
        try:
            if self.proxies:
                response = requests.request(method,
                                            url,
                                            data=data,
                                            params=params,
                                            headers=headers,
                                            auth=self.auth,
                                            verify=self.verify_certificate,
                                            proxies=self.proxies)
            else:
                response = requests.request(method,
                                            url,
                                            data=data,
                                            params=params,
                                            headers=headers,
                                            auth=self.auth,
                                            verify=self.verify_certificate)
            # 502 Bad Gateway triggers retries, since the proxy web
            # server (eg Nginx or Apache) in front of MyTardis could be
            # temporarily restarting
            if response.status_code == 502:
                self.__raise_request_exception(response)
            else:
                response.raise_for_status()
        except requests.exceptions.RequestException as err:
            logger.error("Request failed : %s : %s", err, url)
            raise err
        except Exception as err:
            logger.error(f'Error, {err.msg}, occurred when attempting to call api request {url}')
            raise err
        return response

    def get_mytardis_id(self,
                        mytardis_id):
        response = self.__rest_api_call('GET',
                                        f'{self.project_db_url}/mytardis/{mytardis_id}')
        return response

    def get_project_from_code(self,
                              code):
        response = self.__rest_api_call('GET',
                                        f'{self.project_db_url}/project/findByCode/{code}')
        return response

    def get_people_from_project(self,
                                project_db_id):
        response = self.__rest_api_call('GET',
                                        f'{self.project_db_url}/project/{project_db_id}/member')
        return response

    def get_upi_from_person_id(self,
                               person_id):
        response = self.__rest_api_call('GET',
                                        f'{self.project_db_url}/person/{person_id}/identity')
        return response.json()[0]['username'].split('@')[0]

    def get_project_id_from_code(self,
                                 code):
        try:
            response = self.get_project_from_code(code)
        except Exception as error:
            logger.error(error.message)
            raise
        if repsonse.status >= 300:
            raise FileNotFoundError
        return response.json()['id']

    def __get_person_id_from_uri(self,
                                 uri):
        data = uri.split('/')
        return data[-1]

    def get_people_ids_from_project(self,
                                    project_db_id,
                                    roles=None): # roles is a list of personrole numbers that will be
                                                 # used to limit types of people returned
        response = self.get_people_from_project(project_db_id)
        resp = response.json()
        ids = []
        for person in resp:
            current = person['person']['href']
            if roles:
                current_role = person['role']['href']
                current_role_id = self.__get_person_id_from_uri(current_role)
                if int(current_role_id) in roles or current_role_id in roles:
                    ids.append(self.__get_person_id_from_uri(current))
            else:
                ids.append(self.__get_person_id_from_uri(current))
        return ids

    def get_name_and_description_by_project_id(self,
                                               project_db_id):
        try:
            response = self.__rest_api_call('GET',
                                            f'{self.project_db_url}/project/{project_db_id}')
        except Exception as error:
            logger.error(error)
            raise
        return (response.json()['title'], response.json()['description'], response.json()['creation_date'])
        
    def create_project_in_project_db(self,
                                     title: str,
                                     description: str,
                                     start_date: str,
                                     end_date: str,
                                     division: str,
                                     additional: dict = {}):
        post_data = additional
        post_data['title'] = title
        post_data['description'] = description
        post_data['start_date'] = start_date
        post_data['end_date'] = end_date
        post_data['status_id'] = 1
        post_data['division'] = division
        post_json = dict_to_json(post_data)
        project_id = None
        try:
            response = self.__rest_api_call('POST',
                                            f'{self.project_db_url}/project',
                                            data = post_json)
        except Exception as error:
            logger.error(error.message)
            raise
        if response.status_code == 201:
            project_id = response.headers['Content-Location'].split('/')[-1]
        return project_id

    def add_project_raid(self,
                         project_id: int,
                         project_raid: str):
        data_dict = {'code': project_raid}
        data_json = dict_to_json(data_dict)
        code_id = None
        try:
            response = self.__rest_api_call('POST',
                                            f'{self.project_db_url}/project/{project_id}/code',
                                            data = data_json)
        except Exception as error:
            logger.error(error)
            raise
        if response.status_code == 201:
            code_id = response.headers['Content-Location'].split('/')[-1]
        return code_id

    def post_mytardis_service(self,
                              facility: str):
        data_dict = {'facility': facility}
        data_json = dict_to_json(data_dict)
        service_id = None
        try:
            response = self.__rest_api_call('POST',
                                            f'{self.project_db_url}/mytardis',
                                            data=data_json)
        except Exception as error:
            logger.error(error)
            raise
        if response.status_code == 201:
            service_id = response.headers['Content-Location'].split('/')[-1]
        return service_id
    
    def link_mytardis_to_project(self,
                                 service_id: int,
                                 project_raid: str,
                                 start_date: str):
        data_dict = {'first_day': start_date,
                     'project_code': project_raid}
        data_json = dict_to_json(data_dict)
        try:
            response = self.__rest_api_call('POST',
                                            f'{self.project_db_url}/mytardis/{service_id}/project',
                                            data=data_json)
        except Exception as error:
            logger.error(error)
            raise
        return response

    def get_project_by_id(self,
                          project_id: int):
        try:
            response = self.__rest_api_call('GET',
                                            f'{self.project_db_url}/project/{project_id}')
        except Exception as error:
            logger.error(error)
            raise
        return response

    def get_project_codes(self,
                          project_id: int):
        try:
            response = self.__rest_api_call('GET',
                                            f'{self.project_db_url}/project/{project_id}/code')
        except Exception as error:
            logger.error(error)
            raise
        return response

    def get_project_services(self,
                          project_id: int):
        try:
            response = self.__rest_api_call('GET',
                                            f'{self.project_db_url}/project/{project_id}/service')
        except Exception as error:
            logger.error(error)
            raise
        return response

    def get_mytardis_services(self):
        try:
            response = self.__rest_api_call('GET',
                                            f'{self.project_db_url}/mytardis')
        except Exception as error:
            logger.error(error)
            raise
        return response
