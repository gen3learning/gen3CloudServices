from cloud_services import common_utils
from cloud_services.aws.aws_redshift import redshift_main
from cloud_services.aws.aws_common import get_credentials
import redshift_connector as rc

class main():
    def __init__(self, awsregion):
        self.service_name = 'redshift' #service name
        self.data_service_name = 'redshift-data' #service name
        self.region = awsregion
        self.service_object = {}
        self.response = {}
        self.service_client = None
        self.sts_client = None
        self.current_session = None
        self.client_status = None
        self.client_exception_details = None
        self.client_status_messsage = None
        self.cred_status = None
        self.cred_exception_details = None
        self.cred_status_message = None
        self.data_service_client = None
        self.redshift_query = None
        self.redshift_cursor = None
        self.error_id = None
        self.redshift_connection = None

    def create_service_client_resource(self):
        credentials_class = get_credentials.aws_credentials()
        credentials_class.get_credentials(self.service_object)
        self._access_key = get_credentials.aws_credentials.aws_access_key
        self._secret_key = get_credentials.aws_credentials.aws_secret_access_key
        self._session_token = get_credentials.aws_credentials.aws_session_token
        required_parameters = [
            "cluster_identifier",
            "database_name",
            "database_user",
            "query"
        ]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            connection_response = common_utils.missing_key_response(missing_parameters)
            self.client_status = False
        else:
            try:
                self.redshift_connection = rc.connection(
                    iam=True,
                    database=self.service_object['database_name'],
                    cluster_identifier=self.service_object['cluster_identifier'],
                    db_user=self.service_object['database_user'],
                    user='',
                    password='',
                    access_key_id=self._access_key,
                    secret_access_key=self._secret_key,
                    session_token=self._session_token,
                    region=self.region
                )
                self.client_status = True
            except Exception as exception_details:
                self.client_status = False
                self.client_exception_details = exception_details

            if self.client_status:
                self.redshift_cursor = self.redshift_connection.cursor()
            else:
                self.client_status_messsage = "Unable to set connection with the redshift cluster"

    def set_service_full_object(self, service_object):
        self.service_object = service_object

    def set_service_object_key(self, object_key, object_value):
        self.service_object[object_key] = object_value

    def get_service_object(self):
        return self.service_object

    def get_service_object_key(self, object_key):
        return self.service_object[object_key]

    # End of common functions

    # start of functions unique to individual services

    def execute_query(self):
        required_parameters = [
            "cluster_identifier"
        ]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            describe_statement_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                try:
                    response = self.redshift_cursor.execute(self.service_object['query_id'])
                    query_data = self.redshift_cursor.fetch_dataframe()
                    query_result_response = {
                        "status": True,
                        "returned_by": "jdbc",
                        "df": query_data
                    }
                except Exception as exception_details:
                    query_result_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                query_result_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return query_result_response

    # end of functions unique to individual services

# Main Entry
def service_api(service_object):
    # start of common code for all services - section 1
    response = {
        "status": False
    }
    if 'region' in service_object:
        service_class = main(service_object['region'])
        service_api_class = redshift_main.main(service_object['region'])
        service_class.set_service_full_object(service_object)
        service_class.create_service_client_resource()
        if 'action' in service_object:
            # end of common code for all services - section 1
            # start of service specific function calls - section 2
            service_action = service_object['action']
            service_action_response = service_action + "_response"
            if service_class.client_status:
                if service_action == 'submit_and_get_query_results':
                    get_cluster_response = service_class.execute_query()
                    response["status"] = get_cluster_response["status"]
                    response[service_action_response] = get_cluster_response

                else:
                    api_response = service_api_class.service_api(service_object)
                    response["status"] = api_response["status"]
                    response[service_action_response] = api_response
            else:
                response = common_utils.error_exception_message(service_object,[
                        service_class.data_client_exception_details,
                        service_class.client_exception_details
                    ])
        else:
            response = common_utils.error_exception_message(service_object,"Missing Key 'Action")
    else:
        response = common_utils.error_exception_message(service_object,"Missing Key 'Region")
    return response
    # end of service specific function calls - section 3