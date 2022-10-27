import logging
from cloud_services import common_utils
from cloud_services.aws.aws_common import connect_to_aws
import inspect

class main():
    def __init__(self, awsregion):
        self._logger: logging.Logger = logging.getLogger(__name__)
        self.service_name = 'stepfunctions' #service name
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

    def list_executions(self):
        required_parameters = ["step_function_arn"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            list_executions_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                try:
                    exceutions_list = []
                    execution_dictionary = {}
                    paginator = self.service_client.get_paginator('list_executions')
                    response = paginator.paginate(stateMachineArn=self.service_object['step_function_arn'])
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        list_executions_response = common_utils.error_exception_message(self.service_object,
                                                                                     response)
                    else:
                        for contents in response['executions']:
                            if not (contents is None):
                                exceutions_list.append(contents['name'])
                                execution_dictionary['name'] = {
                                    "execution_arn": contents["executionArn"],
                                    "status": contents['status'],
                                    "startDate": contents['startDate'],
                                    "stopDate": contents['stopDate']
                                }
                        list_executions_response = {
                            "status": True,
                            "execution_count": len(exceutions_list),
                            "execution_list": exceutions_list,
                            "execution_dictionary": execution_dictionary
                        }
                        self.close_connections()
                except Exception as exception_details:
                    list_executions_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                list_executions_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)

        return list_executions_response

    def list_execution_by_status(self):
        required_parameters = ["step_function_arn",'status']
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            list_execution_by_status_response = common_utils.missing_key_response(missing_parameters)
        else:
            list_executions_response = self.list_executions()
            if self.list_executions_response["status"]:
                response = common_utils.search_json_objects(list_executions_response['execution_dictionary'],'status',self.service_object['status'])
                list_execution_by_status_response = {
                    "status": True,
                    "execution_count": len(response),
                    "execution_list": response
                }
                self.close_connections()
            else:
                list_execution_by_status_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return list_execution_by_status_response

    def get_execution_detail(self):
        required_parameters = ["execution_arn"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            get_execution_detail_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                try:
                    response = self.service_client.describe_execution(executionArn=self.service_object["execution_arn"])
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        get_execution_detail_response = common_utils.error_exception_message(self.service_object,
                                                                                     response)
                    else:
                        get_execution_detail_response = {
                            "status": True,
                            "execution_status": response['status'],
                            "execution_start_date": response['startDate'],
                            "execution_end_date": response['stopDate'],
                            "execution_inputs": response['input'],
                            "execution_outputs": response['output']
                        }
                    self.close_connections()
                except Exception as exception_details:
                    get_execution_detail_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                get_execution_detail_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return get_execution_detail_response

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
            if service_action == 'get_list_of_executions':
                object_response = service_class.list_executions()
                response["status"] = object_response["status"]
                response[service_action_response] = object_response
            elif service_action == 'get_completed_executions':
                service_object['status'] = "SUCCEEDED"
                object_response = service_class.list_execution_by_status()
                response["status"] = object_response["status"]
                response[service_action_response] = object_response
            elif service_action == 'get_failed_executions':
                service_object['status'] = "FAILED"
                object_response = service_class.list_execution_by_status()
                response["status"] = object_response["status"]
                response[service_action_response] = object_response
            elif service_action == 'get_running_executions':
                service_object['status'] = "RUNNING"
                object_response = service_class.list_execution_by_status()
                response["status"] = object_response["status"]
                response[service_action_response] = object_response
            elif service_action == 'get_timedout_executions':
                service_object['status'] = "TIMED_OUT"
                object_response = service_class.list_execution_by_status()
                response["status"] = object_response["status"]
                response[service_action_response] = object_response
            elif service_action == 'get_aborted_executions':
                service_object['status'] = "ABORTED"
                object_response = service_class.list_execution_by_status()
                response["status"] = object_response["status"]
                response[service_action_response] = object_response
            elif service_action == 'get_execution_details':
                object_response = service_class.get_execution_detail()
                response["status"] = object_response["status"]
                response[service_action_response] = object_response
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