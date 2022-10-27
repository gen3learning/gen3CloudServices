import time
import logging
from cloud_services import common_utils
from cloud_services.aws.aws_common import connect_to_aws
from cloud_services.aws.aws_s3 import s3_main
import inspect

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class main():
    def __init__(self, awsregion):
        self._logger: logging.Logger = logging.getLogger(__name__)
        self.service_name = 'athena' #service name
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

    def get_default_athena_query_results_bucket(self, application_name):
        if application_name is None:
            application_name = ""
        else:
            application_name = application_name + "/"

        self.service_object['athena_query_result_bucket'] = 's3://aws-athena-query-results-' + self.service_object['region'] + "-" + self.service_object['account_id'] + "/" + application_name

    def get_catalogs(self):
        if self.client_status:
            data_catalog_response = {}
            try:
                data_catalogs = []
                response = self.service_client.list_data_catalogs()
                for catalogs in response['DataCatalogsSummary']:
                    data_catalogs.append(catalogs['CatalogName'])
                while 'NextToken' in response:
                    response = self.service_client.list_data_catalogs(
                        NextToken=response['NextToken']
                    )
                    for catalogs in response['DataCatalogsSummary']:
                        data_catalogs.append(catalogs['CatalogName'])
                if len(data_catalogs) > 0:
                    data_catalog_response['status'] = True
                    data_catalog_response['catalogs'] = data_catalogs
                    data_catalog_response['catalog_count'] = len(data_catalogs)
                else:
                    data_catalog_response['status'] = False
                    data_catalog_response['catalog_count'] = 0
            except Exception as exception_details:
                data_catalog_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            self.close_connections()
        else:
            data_catalog_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return data_catalog_response

    def get_schemas(self):
        required_parameters = ["catalog_name"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            get_schemas_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                get_schemas_response = {}
                try:
                    paginator = self.service_client.get_paginator('list_databases')
                    response = paginator.paginate(CatalogName=self.service_object['catalog_name'])
                    page_status = 0
                    for page in response:
                        page_status = page['ResponseMetadata']['HTTPStatusCode']
                    if page_status != 200:
                        get_schemas_response = common_utils.error_exception_message(self.service_object,response)
                    else:
                        database_list = []
                        for page in response:
                            for database_name in page['DatabaseList']:
                                database_list.append(database_name['Name'])
                        if len(database_list) > 0:
                            get_schemas_response['status'] = True
                            get_schemas_response['databases'] = database_list
                            get_schemas_response['database_count'] = len(database_list)
                        else:
                            get_schemas_response['Status'] = True
                            get_schemas_response['database_count'] = 0
                except Exception as exception_details:
                    get_schemas_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
                self.close_connections()
            else:
                get_schemas_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return get_schemas_response

    def get_tables(self):
        required_parameters = ["catalog_name","database_name"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            get_tables_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                get_tables_response = {}
                try:
                    paginator = self.service_client.get_paginator('list_table_metadata')
                    response = paginator.paginate(
                        CatalogName=self.service_object['catalog_name'],
                        DatabaseName=self.service_object['database_name']
                    )
                    page_status = 0
                    for page in response:
                        page_status = page['ResponseMetadata']['HTTPStatusCode']
                    if page_status != 200:
                        get_tables_response = common_utils.error_exception_message(self.service_object,response)
                    else:
                        table_list = []
                        for page in response:
                            for table_detail in page['TableMetadataList']:
                                table_list.append(table_detail['Name'])

                        if len(table_list) > 0:
                            get_tables_response['status'] = True
                            get_tables_response['tables'] = table_list
                            get_tables_response['table_count'] = len(table_list)
                        else:
                            get_tables_response['Status'] = True
                            get_tables_response['table_count'] = 0
                except Exception as exception_details:
                    get_tables_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
                self.close_connections()
            else:
                get_tables_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return get_tables_response

    def get_columns(self):
        required_parameters = ["catalog_name","database_name","table_name"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            get_columns_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                get_columns_response = {}
                try:
                    paginator = self.service_client.get_paginator('list_table_metadata')
                    response = paginator.paginate(
                        CatalogName=self.service_object['catalog_name'],
                        DatabaseName=self.service_object['database_name'],
                        Expression=self.service_object['table_name']
                    )
                    page_status = 0
                    for page in response:
                        page_status = page['ResponseMetadata']['HTTPStatusCode']
                    if page_status != 200:
                        get_columns_response = common_utils.error_exception_message(self.service_object,response)
                    else:
                        column_list = []
                        field_dict = {}
                        field_description = {}
                        for page in response:
                            for column_detail in page['TableMetadataList'][0]['Columns']:
                                column_name = column_detail['Name']
                                column_list.append(column_name)
                                field_dict[column_name] = column_detail['Type']
                                if 'Comment' in column_detail:
                                    field_description[column_name] = column_detail['Comment']
                                else:
                                    field_description[column_name] = 'No Description'

                        if len(column_list) > 0:
                            get_columns_response['status'] = True
                            get_columns_response['columns'] = column_list
                            get_columns_response['column_count'] = len(column_list)
                        else:
                            get_columns_response['Status'] = True
                            get_columns_response['column_count'] = 0
                except Exception as exception_details:
                    get_columns_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
                self.close_connections()
            else:
                get_columns_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return get_columns_response

    def get_workgroups(self):
        if self.client_status:
            get_workgroups_response = {}
            try:
                workgroup_list = []
                response = self.service_client.list_work_groups()
                for workgroup_detail in response['WorkGroups']:
                    if workgroup_detail['State'] == 'ENABLED':
                        workgroup_list.append(workgroup_detail['Name'])
                while 'NextToken' in response:
                    for workgroup_detail in response['WorkGroups']:
                        if workgroup_detail['State'] == 'ENABLED':
                            workgroup_list.append(workgroup_detail['Name'])
                    next_token = response['NextToken']
                    response = self.service_client.list_work_groups(NextToken=next_token)

                if len(workgroup_list) > 0:
                    get_workgroups_response['status'] = True
                    get_workgroups_response['workgroups'] = workgroup_list
                    get_workgroups_response['workgroup_count'] = len(workgroup_list)
                else:
                    get_workgroups_response['status'] = False
                    get_workgroups_response['workgroup_count'] = 0
            except Exception as exception_details:
                get_workgroups_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            self.close_connections()
        else:
            get_workgroups_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return get_workgroups_response

    def get_named_queries(self):
        required_parameters = ["workgroup_name"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            get_named_queries_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                get_named_queries_response = {}
                try:
                    paginator = self.service_client.get_paginator('list_named_queries')
                    response = paginator.paginate()
                    page_status = 0
                    for page in response:
                        page_status = page['ResponseMetadata']['HTTPStatusCode']
                    if page_status != 200:
                        get_named_queries_response = common_utils.error_exception_message(self.service_object,response)
                    else:
                        named_query_list = []
                        for page in response:
                            for named_query in page['NamedQueryIds']:
                                named_query_list.append(named_query)
                    if len(named_query_list) > 0:
                        get_named_queries_response['status'] = True
                        get_named_queries_response['named_queries'] = named_query_list
                        get_named_queries_response['named_query_count'] = len(named_query_list)
                    else:
                        get_named_queries_response['Status'] = False
                        get_named_queries_response['named_query_count'] = 0
                        get_named_queries_response = common_utils.error_exception_message(self.service_object, response, inspect.currentframe().f_back.f_lineno)
                except Exception as exception_details:
                    get_named_queries_response = common_utils.error_exception_message(self.service_object,
                                                                                   exception_details,
                                                                                   inspect.currentframe().f_back.f_lineno)
                self.close_connections()
            else:
                get_named_queries_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return get_named_queries_response


    def get_named_query_detail(self):
        if self.client_status:
            get_named_query_detail_response = {}
            try:
                named_query_detail_list = []
                for named_query_id in self.service_object['named_queries']:
                    named_query_detail = {}
                    named_query_detail[named_query_id] = {}
                    response = self.service_client.get_named_query(
                        NamedQueryId=named_query_id
                    )
                    if 'NamedQuery' in response:
                        named_query_detail[named_query_id]['status'] = True
                        named_query_detail[named_query_id]['query_name'] = response['NamedQuery']['Name']
                        named_query_detail[named_query_id]['query_string'] = response['NamedQuery']['QueryString']
                        named_query_detail[named_query_id]['workgroup_name'] = response['NamedQuery']['WorkGroup']
                        named_query_detail[named_query_id]['database'] = response['NamedQuery']['Database']
                        named_query_detail_list.append(named_query_detail)
                    else:
                        named_query_detail[named_query_id]['status'] = False
                    get_named_query_detail_response = {
                        'status': True,
                        'named_queries': named_query_detail_list,
                        'named_query_count': len(named_query_detail_list)
                    }
            except Exception as exception_details:
                get_named_query_detail_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            self.close_connections()
        else:
            get_named_query_detail_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return get_named_query_detail_response

    def create_named_query(self):
        required_parameters = ["named_query_name","database_name","query_string"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            create_named_query_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                if 'description' in self.service_object:
                    if len(self.service_object['description']) == 0 or self.service_object['description'] is None:
                        self.service_object['description'] = 'Named Query'
                else:
                    self.service_object['description'] = 'Named Query'

                if '"workgroup_name"' in self.service_object:
                    if len(self.service_object["workgroup_name"]) == 0 or self.service_object['"workgroup_name"'] is None:
                        self.service_object["workgroup_name"] = 'primary'
                else:
                    self.service_object["workgroup_name"] = 'primary'
                create_named_query_response = {}
                try:
                    response = self.service_client.create_named_query(
                        Name=self.service_object['named_query_name'],
                        Description=self.service_object['description'],
                        Database=self.service_object['database_name'],
                        QueryString=self.service_object['query_string'],
                        WorkGroup=self.service_object['workgroup_name']
                    )
                    create_named_query_response['status'] = True
                    create_named_query_response['create_named_query_response'] = response
                except Exception as exception_details:
                    create_named_query_response = common_utils.error_exception_message(self.service_object,
                                                                                   exception_details,
                                                                                   inspect.currentframe().f_back.f_lineno)
                self.close_connections()
            else:
                create_named_query_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return create_named_query_response

    def run_athena_query(self):
        required_parameters = ["athena_query_output_location","query"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            athena_run_query_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                if 'workgroup_name' in self.service_object:
                    if self.service_object['workgroup_name'] == '':
                        self.service_object['workgroup_name'] = 'primary'

                if 'catalog_name' in self.service_object:
                    if self.service_object['catalog_name'] == '':
                        self.service_object['catalog_name'] = 'AwsDataCatalog'

                permitted_query_list = self.service_object['query'].split(" ")[0]
                if permitted_query_list.lower() not in ['select','with']:
                    athena_run_query_response = common_utils.error_exception_message(self.service_object,"Only Select Queries can be executed in G3Query")
                else:
                    try:
                        query_id = self.service_client.start_query_execution(
                            QueryString=self.service_object['query'],
                            QueryExecutionContext={
                                'Catalog': self.service_object['catalog_name']
                            },
                            ResultConfiguration={
                                'OutputLocation': self.service_object['athena_query_output_location']
                            },
                            WorkGroup=self.service_object['workgroup_name']
                        )['QueryExecutionId']
                        query_status = None
                        while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
                            query_execution_response = self.service_client.get_query_execution(QueryExecutionId=query_id)
                            query_execution_details = query_execution_response['QueryExecution']
                            query_status = query_execution_details['Status']['State']
                            athenas3outputlocation = query_execution_response['QueryExecution']['ResultConfiguration']['OutputLocation']
                            if query_status == 'FAILED' or query_status == 'CANCELLED':
                                query_state_change_reason = query_execution_details['Status']['StateChangeReason']
                                raise Exception('Athena query failed, error details: "{}"'.format(query_state_change_reason))
                            time.sleep(10)

                        output_bucket_object = common_utils.get_bucket_name_from_s3_path(s3_full_path=athenas3outputlocation,path_contains_object=True)
                        output_bucket_name = output_bucket_object["bucket_name"]
                        output_prefix = output_bucket_object["prefix"]
                        try:
                            service_object = {
                                "cloud_provider_name": self.service_object['cloud_provider_name'],
                                "region": self.service_object['region'],
                                "profile": self.service_object['profile'],
                                "service_name": "s3",
                                "action": "read_s3_file",
                                "file_type": "csv",
                                "bucket_name": output_bucket_name,
                                "prefix_name": output_prefix
                            }
                            athena_run_query_response = s3_main.service_api(service_object)
                        except Exception as exception_details:
                            athena_run_query_response = common_utils.error_exception_message(self.service_object,
                                                                                           exception_details,
                                                                                           inspect.currentframe().f_back.f_lineno)
                    except Exception as exception_details:
                        athena_run_query_response = common_utils.error_exception_message(self.service_object,
                                                                                           exception_details,
                                                                                           inspect.currentframe().f_back.f_lineno)
            else:
                athena_run_query_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return athena_run_query_response

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
            if service_action == 'list_catalogs':
                get_catalog_response = service_class.get_catalogs()
                response["status"] = get_catalog_response["status"]
                response[service_action_response] = get_catalog_response

            elif service_action == 'list_schemas':
                get_schemas_response = service_class.get_schemas()
                response["status"] = get_schemas_response["status"]
                response[service_action_response] = get_schemas_response

            elif service_action == 'list_tables':
                get_tables_response = service_class.get_tables()
                response["status"] = get_tables_response["status"]
                response[service_action_response] = get_tables_response

            elif service_action == 'list_table_columns':
                get_columns_response = service_class.get_columns()
                response["status"] = get_columns_response["status"]
                response[service_action_response] = get_columns_response

            elif service_action == 'list_workgroups':
                get_workgroups_response = service_class.get_workgroups()
                response["status"] = get_workgroups_response["status"]
                response[service_action_response] = get_workgroups_response

            elif service_action == 'get_named_queries':
                get_named_queries_response = service_class.get_named_queries()
                if get_named_queries_response["status"] and get_named_queries_response['named_query_count'] > 0:
                    service_class.set_service_object_key('named_queries', get_named_queries_response['named_queries'])
                    get_named_query_detail_response = service_class.get_named_query_detail()
                    response["status"] = get_named_query_detail_response["status"]
                    response[service_action_response] = get_named_query_detail_response

            elif service_action == 'create_named_query':
                create_named_query_response = service_class.create_named_query()
                response["status"] = create_named_query_response["status"]
                response[service_action_response] = create_named_query_response

            elif service_action == 'run_named_query':
                if 'named_queries' not in service_object:
                    response = common_utils.error_exception_message(service_object, "Missing Keys 'Action'",
                                                                    inspect.currentframe().f_back.f_lineno)
                else:
                    service_class.set_service_object_key('named_queries', [service_object['named_queries']])
                    get_named_query_detail_response = service_class.get_named_query_detail()
                    if get_named_query_detail_response["status"]:
                        query_string = get_named_query_detail_response['named_queries'][0][service_object['named_queries'][0]]['query_string']
                        service_class.set_service_object_key('query', query_string + " limit 10")
                    run_athena_query_response = service_class.run_athena_query()
                    response["status"] = run_athena_query_response["status"]
                    response[service_action_response] = run_athena_query_response

            elif service_action == 'run_query':
                run_athena_query_response = service_class.run_athena_query()
                response["status"] = run_athena_query_response["status"]
                response[service_action_response] = run_athena_query_response

            # end of service specific function calls - section 2
            # start of service specific function calls - section 3
            else:
                response = common_utils.error_exception_message(service_object,"Invalid Action",
                                                                                           inspect.currentframe().f_back.f_lineno)
        else:
            response = common_utils.error_exception_message(service_object,"Missing Keys 'Action'",
                                                                                           inspect.currentframe().f_back.f_lineno)
    else:
        response = common_utils.error_exception_message(service_object,"Missing Keys 'Region",
                                                                                           inspect.currentframe().f_back.f_lineno)
    return response
    # end of service specific function calls - section 3