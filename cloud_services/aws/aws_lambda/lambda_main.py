import logging
from cloud_services import common_utils
from cloud_services.aws.aws_common import connect_to_aws
import json
import typing
import inspect
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class main():
    def __init__(self, awsregion):
        self._logger: logging.Logger = logging.getLogger(__name__)
        self.service_name = 'lambda' #service name
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

    def invoke_lambda_function(self):
        required_parameters = ["function_name","invocation_type"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            invoke_lambda_function_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                invoke_lambda_function_response = {}
                try:
                    if "payload" not in self.service_object:
                        input_payload = {
                            "key1":"value1",
                            "key2":"value2"
                        }
                    else:
                        input_payload = self.service_object["payload"]
                    payload_string = json.dumps(input_payload)
                    response = self.service_client.invoke(
                        FunctionName=self.service_object['function_name'],
                        InvocationType=self.service_object['invocation_type'],
                        Payload=payload_string
                    )
                    if response['ResponseMetadata']['HTTPStatusCode'] != self.service_object['expected_return_code']:
                        invoke_lambda_function_response = common_utils.error_exception_message(self.service_object, response, inspect.currentframe().f_back.f_lineno)
                    else:
                        lambda_streaming_response = response["Payload"].read().decode('utf8')
                        print(response)
                        if self.service_object['invocation_type'] != 'RequestResponse':
                            invoke_lambda_function_response['request_id'] = response['ResponseMetadata']['RequestId']
                        else:
                            invoke_lambda_function_response['response'] = lambda_streaming_response
                        invoke_lambda_function_response['status'] = True
                except Exception as exception_details:
                    invoke_lambda_function_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                invoke_lambda_function_response = common_utils.error_exception_message(self.service_object,self.client_exception_details, inspect.currentframe().f_back.f_lineno)
        self.close_connections()
        return invoke_lambda_function_response

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
            if service_action == 'invoke_lambda_and_get_response':
                service_class.set_service_object_key("invocation_type","RequestResponse")
                service_class.set_service_object_key("expected_return_code",200)
                invoke_lambda_response = service_class.invoke_lambda_function()
                response["status"] = invoke_lambda_response["status"]
                response[service_action_response] = invoke_lambda_response

            elif service_action == 'invoke_lambda':
                service_class.set_service_object_key("invocation_type","Event")
                service_class.set_service_object_key("expected_return_code",202)
                invoke_lambda_response = service_class.invoke_lambda_function()
                response["status"] = invoke_lambda_response["status"]
                response[service_action_response] = invoke_lambda_response


            elif service_action == 'invoke_lambda_dry_run':
                service_class.set_service_object_key("invocation_type","DryRun")
                service_class.set_service_object_key("expected_return_code",204)
                invoke_lambda_response = service_class.invoke_lambda_function()
                response["status"] = invoke_lambda_response["status"]
                response[service_action_response] = invoke_lambda_response

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