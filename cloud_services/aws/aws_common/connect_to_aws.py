import sys
#sys.path.insert(0,'/glue/lib/installation')
keys = [k for k in sys.modules.keys() if 'boto' in k]
for k in keys:
    if 'boto' in k:
        del sys.modules[k]

import boto3
print(f"Version of installed boto 3 is {boto3.__version__}")

import logging
from cloud_services.aws.aws_common import get_credentials
from botocore.client import Config

class aws_connect(get_credentials.aws_credentials):
    def __init__(self, service_name, aws_region, connection_object):
        self._logger: logging.Logger = logging.getLogger(__name__)
        self.service_name = service_name
        self.connection_status = None
        self.aws_region = aws_region
        credentials_class = get_credentials.aws_credentials()
        credentials_class.get_credentials(connection_object)
        self.__session_access_key = get_credentials.aws_credentials.aws_access_key
        self.__session_secret_key = get_credentials.aws_credentials.aws_secret_access_key
        self.__session_token = get_credentials.aws_credentials.aws_session_token
        self.service_client = None
        self.service_resource = None
        self.client_exception_details = None
        self.resource_exception_details = None
        self.client_status = None
        self.resource_status = None
        self.connection_config = Config(
            read_timeout=300,
            connect_timeout=5,
            retries={'max_attempts': 0}
        )
        self.credentials_response = get_credentials.aws_credentials.credentials_response
        self.create_service_client()
        if service_name.lower() in ['s3']:
            self.create_service_resource()
        self.sts_client = get_credentials.aws_credentials.sts_client

    def create_service_client(self):
        if get_credentials.aws_credentials.credentials_response["status"]:
            try:
                self.service_client = get_credentials.aws_credentials.current_session.client(self.service_name,
                                                             config=self.connection_config)
                self.client_status = True
            except Exception as exception_details:
                self.client_status = False
                self.client_exception_details = exception_details
        else:
            self.client_status = False
            self.client_exception_details = get_credentials.aws_credentials.credentials_response['exception_details']

    # use this function only for services where you can create a resource
    def create_service_resource(self):
        if get_credentials.aws_credentials.credentials_response["status"]:
            try:
                self.service_resource = get_credentials.aws_credentials.current_session.resource(self.service_name,
                                                             config=self.connection_config)
                self.resource_status = True
            except Exception as exception_details:
                self.resource_status = False
                self.resource_exception_details = exception_details
        else:
            self.resource_status = False
            self.resource_exception_details = get_credentials.aws_credentials.credentials_response['exception_details']
