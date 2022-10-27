import logging
from cloud_services import common_utils
from cloud_services.aws.aws_common import connect_to_aws
import inspect

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class main():
    def __init__(self, awsregion):
        self._logger: logging.Logger = logging.getLogger(__name__)
        self.service_name = 'events' #service name
        self.region = awsregion
        self.service_object = {}
        self.response = {}
        self.service_client = None
        self.sts_client = None
        self.current_session = None
        self.client_status = None
        self.sts_client_status = None
        self.client_exception_details = None
        self.client_status_messsage = None
        self.cred_status = None
        self.cred_exception_details = None
        self.cred_status_message = None
        self.service_resource = None
        self.resource_status = None
        self.resource_exception_details = None
        self.resource_status_messsage = None

    def create_service_client_resource(self):
        aws_connect_class = connect_to_aws.aws_connect(self.service_name, self.region, self.service_object)
        if aws_connect_class.service_client is not None:
            self.service_client = aws_connect_class.service_client
            self.client_status = True
        else:
            self.client_status = False

        if aws_connect_class.sts_client is not None:
            self.sts_client = aws_connect_class.sts_client
            self.sts_client_status = True
        else:
            self.sts_client_status = False

        if aws_connect_class.service_resource is not None:
            self.service_resource = aws_connect_class.service_resource
            self.resource_status = True
        else:
            self.resource_status = False

    def set_service_full_object(self, service_object):
        self.service_object = service_object

    def set_service_object_key(self, object_key, object_value):
        self.service_object[object_key] = object_value

    def get_service_object(self):
        return self.service_object

    def get_service_object_key(self, object_key):
        return self.service_object[object_key]

    def close_connections(self):
        if self.client_status:
            self.service_client.close()
        if self.sts_client_status:
            self.sts_client.close()

    # End of common functions

    # start of functions unique to individual services

    def put_event(self):
        required_parameters = ["event_source","event_detail_type","event_detail"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            put_event_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                put_event_response = {}
                try:
                    if 'event_bus_name' not in self.service_object:
                        self.service_object['event_bus_name'] = 'default'
                    response = self.service_client.put_event(
                        Entries = [
                            {
                                "Source": self.service_object['event_source'],
                                "DetailType": self.service_object['event_detail_type'],
                                "Detail": self.service_object['event_detail'],
                                "EventBusName": self.service_object['event_bus_name']
                            }
                        ]
                    )
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        put_event_response = common_utils.error_exception_message(self.service_object,
                                                                                    response)
                    else:
                        put_event_response['Status'] = True
                        put_event_response['event_response'] = response
                    self.close_connections()
                except Exception as exception_details:
                    put_event_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                put_event_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return put_event_response

    # end of functions unique to individual services

# Main Entry
def service_api(service_object):
    # start of common code for all services - section 1
    response = {
        "status": False
    }
    if 'region' in service_object:
        service_class = main(service_object['region'])
        service_class.set_service_full_object(service_object)
        service_class.create_service_client_resource()
        if 'action' in service_object:
            # end of common code for all services - section 1
            # start of service specific function calls - section 2
            service_action = service_object['action']
            service_action_response = service_action + "_response"
            if service_action == 'put_event':
                list_table_response = service_class.put_event()
                response["status"] = list_table_response["status"]
                response[service_action_response] = list_table_response

            # end of service specific function calls - section 2
            # start of service specific function calls - section 3
            else:
                response = common_utils.error_exception_message(service_object,"Invalid Action", inspect.currentframe().f_back.f_lineno)
        else:
            response = common_utils.error_exception_message(service_object,"Missing Keys 'Action'", inspect.currentframe().f_back.f_lineno)
    else:
        response = common_utils.error_exception_message(service_object,"Missing Keys 'Region", inspect.currentframe().f_back.f_lineno)
    return response
    # end of service specific function calls - section 3