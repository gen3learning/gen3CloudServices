import logging
import pandas as pd
from cloud_services import common_utils
from cloud_services.aws.aws_common import connect_to_aws
import inspect

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class main():
    def __init__(self, awsregion):
        self._logger: logging.Logger = logging.getLogger(__name__)
        self.service_name = 'quicksight' #service name
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
        self.quicksight_authorized_user = None
        self.assumed_role_session = None

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

    def list_dataset(self):
        required_parameters = ["quicksight_account_id"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            list_dataset_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                list_dataset_response = {}
                try:
                    paginator = self.service_client.get_paginator('list_data_sets')
                    response = paginator.paginate(AwsAccountId=self.service_object['quicksight_account_id'])
                    page_status = 0
                    for page in response:
                        page_status = page['ResponseMetadata']['HTTPStatusCode']
                    if page_status != 200:
                        list_dataset_response = common_utils.error_exception_message(self.service_object, response)
                    else:
                        dataset_list = []
                        for page in response:
                            for dataset_detail in page['DataSetSummaries']:
                                dataset_details = {}
                                dataset_details[dataset_detail['DataSetId']] = {
                                    'Name': dataset_detail['Name']
                                }
                                dataset_list.append(dataset_details)
                        if len(dataset_list) > 0:
                            list_dataset_response['status'] = True
                            list_dataset_response['datasets'] = dataset_list
                            list_dataset_response['dataset_count'] = len(dataset_list)
                        else:
                            list_dataset_response['Status'] = True
                            list_dataset_response['dataset_count'] = 0
                    self.close_connections()
                except Exception as exception_details:
                    list_dataset_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                list_dataset_response = common_utils.error_exception_message(self.service_object, self.client_exception_details, inspect.currentframe().f_back.f_lineno)
        return list_dataset_response

    def describe_dataset(self):
        required_parameters = ["quicksight_account_id","dataset_id"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            describe_dataset_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                describe_dataset_response = {}
                try:
                    response = self.service_client.describe_data_set(
                        AwsAccountId=self.service_object['quicksight_account_id'],
                        DataSetId=self.service_object['dataset_id']
                    )
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        describe_dataset_response = common_utils.error_exception_message(self.service_object, response, inspect.currentframe().f_back.f_lineno)
                    else:
                        describe_dataset_response['dataset_arn'] = response['DataSet']['Arn']
                        describe_dataset_response['Name'] = response['DataSet']['Name']
                        describe_dataset_response['status'] = True
                        describe_dataset_response['PhysicalTableMap'] = response['DataSet']['PhysicalTableMap']
                    self.close_connections()
                except Exception as exception_details:
                    describe_dataset_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                describe_dataset_response = common_utils.error_exception_message(self.service_object, self.client_exception_details, inspect.currentframe().f_back.f_lineno)
        return describe_dataset_response

    def describe_dashboard(self):
        required_parameters = ["quicksight_account_id","dashboard_id"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            describe_dashboard_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                describe_dashboard_response = {}
                try:
                    response = self.service_client.describe_dashboard(
                        AwsAccountId=self.service_object['quicksight_account_id'],
                        DashboardId=self.service_object['dashboard_id']
                    )
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        describe_dashboard_response = common_utils.error_exception_message(self.service_object, response, inspect.currentframe().f_back.f_lineno)
                    else:
                        describe_dashboard_response['Dashboard_arn'] = response['Dashboard']['Arn']
                        describe_dashboard_response['Name'] = response['Dashboard']['Name']
                        describe_dashboard_response['status'] = True
                    self.close_connections()
                except Exception as exception_details:
                    describe_dashboard_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                describe_dashboard_response = common_utils.error_exception_message(self.service_object, self.client_exception_details, inspect.currentframe().f_back.f_lineno)
        return describe_dashboard_response

    def create_dataset(self):
        required_parameters = ["quicksight_account_id","dataset_id","dataset_name","dataset_object","import_mode"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            create_dataset_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                create_dataset_response = {}
                try:
                    response = self.service_client.create_data_set(
                        AwsAccountId=self.service_object['quicksight_account_id'],
                        DataSetId=self.service_object['dataset_id'],
                        Name=self.service_object['dataset_name'],
                        PhysicalTableMap={
                            self.service_object['dataset_object']['PhysicalTableMape']: {
                                "CustomSQL":{
                                    "DataSourceArn": self.service_object['dataset_object'],
                                    "Name": self.service_object['dataset_object']['SQLName'],
                                    "SQLQuery": self.service_object['dataset_object']['SQLQuery'],
                                    "Columns": self.service_object['dataset_object']['Columns']
                                }
                            }
                        },
                        ImportMode=self.service_object['import_mode']
                    )
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        create_dataset_response = common_utils.error_exception_message(self.service_object, response, inspect.currentframe().f_back.f_lineno)
                    else:
                        create_dataset_response['dataset_arn'] = response['DataSet']['Arn']
                        create_dataset_response['Name'] = response['DataSet']['Name']
                        create_dataset_response['status'] = True
                        create_dataset_response['PhysicalTableMap'] = response['DataSet']['PhysicalTableMap']
                    self.close_connections()
                except Exception as exception_details:
                    create_dataset_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                create_dataset_response = common_utils.error_exception_message(self.service_object, self.client_exception_details, inspect.currentframe().f_back.f_lineno)
        return create_dataset_response

    def describe_user(self):
        required_parameters = ["quicksight_account_id","user_name"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            describe_user_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                describe_user_response = {}
                try:
                    response = self.service_client.describe_user(
                        AwsAccountId=self.service_object['quicksight_account_id'],
                        UserName=self.service_object['user_name'], #accountrole/authuser
                        Namespace='default'
                    )
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        describe_user_response = common_utils.error_exception_message(self.service_object, response, inspect.currentframe().f_back.f_lineno)
                    else:
                        describe_user_response['user_arn'] = response['User']['Arn']
                        describe_user_response['user_name'] = response['User']['UserName']
                        describe_user_response['user_role'] = response['User']['Role']
                        describe_user_response['active'] = response['User']['Active']
                        describe_user_response['status'] = True
                    self.close_connections()
                except Exception as exception_details:
                    describe_user_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                describe_user_response = common_utils.error_exception_message(self.service_object, self.client_exception_details, inspect.currentframe().f_back.f_lineno)
        return describe_user_response

    def list_dashboard(self):
        required_parameters = ["quicksight_account_id"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            list_dashboard_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                list_dashboard_response = {}
                try:
                    paginator = self.service_client.get_paginator('list_dashboards')
                    response = paginator.paginate(AwsAccountId=self.service_object['quicksight_account_id'])
                    page_status = 0
                    for page in response:
                        page_status = page['ResponseMetadata']['HTTPStatusCode']
                    if page_status != 200:
                        list_dashboard_response = common_utils.error_exception_message(self.service_object, response)
                    else:
                        dashboard_list = []
                        for page in response:
                            for dashboard_detail in page['DashboardSummaryList']:
                                dashboard_details = {}
                                dashboard_details[dashboard_detail['DashboardId']] = {
                                    'Name': dashboard_detail['Name']
                                }
                                dashboard_list.append(dashboard_details)
                        if len(dashboard_list) > 0:
                            list_dashboard_response['status'] = True
                            list_dashboard_response['dashboards'] = dashboard_list
                            list_dashboard_response['dashboard_count'] = len(dashboard_list)
                        else:
                            list_dashboard_response['Status'] = True
                            list_dashboard_response['dashboard_count'] = 0
                    self.close_connections()
                except Exception as exception_details:
                    list_dashboard_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                list_dashboard_response = common_utils.error_exception_message(self.service_object, self.client_exception_details, inspect.currentframe().f_back.f_lineno)
        return list_dashboard_response

    def open_dashboard(self):
        required_parameters = ["quicksight_account_id","dashboard_id"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            open_dashboard_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                open_dashboard_response = {}
                user_response = self.describe_user()
                print(user_response)
                if user_response['status']:
                    user_arn = user_response['user_arn']
                    if True: #assume_role_client
                        try:
                            if user_response['user_role'] == 'anonymous':
                                response = self.service_client.generate_embed_url_for_anonymous_user(
                                    AwsAccountId=self.service_object['quicksight_account_id'],
                                    Namespace='default',
                                    SessionLifetimeInMinutes=600,
                                    AuthorizedResourceArns=["arn:aws:quicksight:us-east-1:"+self.service_object['quicksight_account_id']+":dashboard/"+self.service_object['dashboard_id']],
                                    ExperienceConfiguration={
                                        'Dashboard': {
                                            'InitialDashboardId': self.service_object['dashboard_id']
                                        }
                                    }
                                )
                            elif user_response['user_role'] not in ['ADMIN','AUTHOR']:
                                response = self.service_client.generate_embed_url_for_registered_user(
                                    AwsAccountId=self.service_object['quicksight_account_id'],
                                    SessionLifetimeInMinutes=600,
                                    UserArn=user_arn,
                                    ExperienceConfiguration={
                                        'Dashboard': {
                                            'InitialDashboardId': self.service_object['dashboard_id']
                                        }
                                    }
                                )
                            else:
                                print(user_arn)
                                response = self.service_client.generate_embed_url_for_registered_user(
                                    AwsAccountId=self.service_object['quicksight_account_id'],
                                    SessionLifetimeInMinutes=600,
                                    UserArn=user_arn,
                                    ExperienceConfiguration={
                                        'QuickSightConsole': {
                                            'InitialPath': '/dashboards/' + self.service_object['dashboard_id']
                                        }
                                    }
                                )
                            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                                open_dashboard_response = common_utils.error_exception_message(self.service_object, response, inspect.currentframe().f_back.f_lineno)
                            else:
                                open_dashboard_response['headers'] = {
                                    "headers": {
                                        "Access-Control-Allow-Origin": "*",
                                        "Access-Control-Allow-Headers": "Content-Type",
                                    }
                                }
                                open_dashboard_response['status'] = True
                                open_dashboard_response['response'] = response
                                open_dashboard_response['isBase64Encoded'] = bool('false')
                            self.close_connections()
                        except Exception as exception_details:
                            open_dashboard_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
                    else:
                        open_dashboard_response = common_utils.error_exception_message(self.service_object,
                                                                                       'unable to assume role for quicksight',
                                                                                       inspect.currentframe().f_back.f_lineno)
                else:
                    open_dashboard_response = common_utils.error_exception_message(self.service_object,
                                                                                   'unable to identify user',
                                                                                   inspect.currentframe().f_back.f_lineno)
            else:
                open_dashboard_response = common_utils.error_exception_message(self.service_object, self.client_exception_details, inspect.currentframe().f_back.f_lineno)
        return open_dashboard_response

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
            if service_action == 'list_datasets':
                list_dataset_response = service_class.list_dataset()
                response["status"] = list_dataset_response["status"]
                response[service_action_response] = list_dataset_response

            elif service_action == 'describe_dataset':
                list_dataset_response = service_class.describe_dataset()
                response["status"] = list_dataset_response["status"]
                response[service_action_response] = list_dataset_response

            elif service_action == 'describe_dashboard':
                list_dataset_response = service_class.describe_dashboard()
                response["status"] = list_dataset_response["status"]
                response[service_action_response] = list_dataset_response

            elif service_action == 'create_dataset':
                list_dataset_response = service_class.create_dataset()
                response["status"] = list_dataset_response["status"]
                response[service_action_response] = list_dataset_response

            elif service_action == 'describe_user':
                list_dataset_response = service_class.describe_user()
                response["status"] = list_dataset_response["status"]
                response[service_action_response] = list_dataset_response

            elif service_action == 'create_dataset':
                list_dataset_response = service_class.create_dataset()
                response["status"] = list_dataset_response["status"]
                response[service_action_response] = list_dataset_response

            elif service_action == 'list_dashboards':
                list_dataset_response = service_class.list_dashboard()
                response["status"] = list_dataset_response["status"]
                response[service_action_response] = list_dataset_response

            elif service_action == 'open_dashboard':
                list_dataset_response = service_class.open_dashboard()
                response["status"] = list_dataset_response["status"]
                response[service_action_response] = list_dataset_response
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