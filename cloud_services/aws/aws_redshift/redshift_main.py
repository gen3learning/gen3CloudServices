from cloud_services import common_utils
from cloud_services.aws.aws_common import connect_to_aws
from botocore.waiter import WaiterModel, WaiterError
import time
import inspect

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
        self.data_client_status = None
        self.sts_client_status = None
        self.client_exception_details = None
        self.client_status_messsage = None
        self.cred_status = None
        self.cred_exception_details = None
        self.cred_status_message = None
        self.data_service_client = None
        self.data_client_status = None
        self.data_client_exception_details = None
        self.data_client_status_messsage = None
        self.redshift_query = None
        self.redshift_cursor = None
        self.error_id = None
        self.redshift_connection = None
        self.describe_table_paginator = None
        self.query_result_paginator - None
        self.waiter_name = 'redshift_data_api_waiter'
        self.time_delay = 2
        max_attempts = 150

        self.waiter_config - {
            'version': 2,
            'waiters': {
                self.waiter_name: {
                    'operation': 'DescribeStatement',
                    'delay':self.time_delay,
                    'maxAttempts':max_attempts,
                    'acceptors': [
                        {
                            "matcher": "path",
                            "expected": "FINISHED",
                            "argument": "Status",
                            "state": "success"
                        },
                        {
                            "matcher": "pathAny",
                            "expected": ["PICKED","STARTED","SUBMITTED"],
                            "argument": "Status",
                            "state": "retry"
                        },
                        {
                            "matcher": "pathAny",
                            "expected": ["FAILED","ABORTED"],
                            "argument": "Status",
                            "state": "failure"
                        }
                    ],
                },
            },
        }
        self.waiter_model = WaiterModel(self.waiter_config)
        self.custom_waiter = None

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

        aws_connect_class = connect_to_aws.aws_connect(self.data_service_name, self.region, self.service_object)
        if aws_connect_class.service_client is not None:
            self.data_service_client = aws_connect_class.service_client
            self.data_client_status = True
        else:
            self.data_client_status = False

    def reset_time_delay(self):
        self.time_delay = 2

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
        if self.data_client_status:
            self.data_service_client.close()
        if self.sts_client_status:
            self.sts_client.close()

    # End of common functions

    # start of functions unique to individual services

    def get_clusters(self, input_cluster_status):
        if self.client_status:
            try:
                cluster_list = []
                cluster_details = {}
                cluster_status_lower = []
                cluster_status_title = []
                for cluster_status_name in input_cluster_status:
                    cluster_status_lower.append(cluster_status_name.lower())
                    cluster_status_title.append(cluster_status_name.title())
                list_of_clusters = self.service_client.describe_clusters()
                if list_of_clusters['ResponseMetadata']['HTTPStatusCode'] != 200:
                    get_clusters_response = common_utils.error_exception_message(self.service_object,
                                                                             list_of_clusters)
                else:
                    for cluster_dt in list_of_clusters['Clusters']:
                        if cluster_dt['ClusterStatus'] in cluster_status_lower and \
                                cluster_dt['ClusterAvailabilityStatus'] in cluster_status_title:
                            cluster_name = cluster_details['ClusterIdentifier']
                            cluster_list.append(cluster_name)
                            cluster_details[cluster_name] = {}
                            cluster_details[cluster_name]['database_name'] = cluster_dt['database_name']
                            cluster_details[cluster_name]['Endpoint_Address'] = cluster_dt['Endpoint']['Address']
                            cluster_details[cluster_name]['Endpoint_Port'] = cluster_dt['Endpoint']['Port']
                    get_clusters_response = {
                        "status": True,
                        "cluster_list": cluster_list,
                        "cluster_details": cluster_details,
                        "cluster_count": len(cluster_list),
                        "account_cluster_count": len(cluster_list)
                    }
            except Exception as exception_details:
                get_clusters_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
        else:
            get_clusters_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        self.close_connections()
        return get_clusters_response

    def list_databases(self):
        required_parameters = ["cluster_identifier","database_name","database_user"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            list_database_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.data_client_status:
                try:
                    db_list = []
                    response = self.data_service_client.list_databases(
                        ClusterIdentifier=self.service_object["cluster_identifier"],
                        Database=self.service_object["database_name"],
                        DbUser=self.service_object["database_user"]
                    )
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        list_database_response = common_utils.error_exception_message(self.service_object,
                                                                                     response)
                    else:
                        while 'NextToken' in response:
                            for db_name in response['Databases']:
                                db_list.append(db_name)
                            response = self.data_service_client.list_databases(
                                ClusterIdentifier=self.service_object["cluster_identifier"],
                                Database=self.service_object["database_name"],
                                DbUser=self.service_object["database_user"],
                                NextToken=response['NextToken']
                            )
                        list_database_response = {
                            "status": True,
                            "database_count": len(db_list),
                            "database_list": db_list
                        }
                except Exception as exception_details:
                    list_database_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                list_database_response = common_utils.error_exception_message(self.service_object,self.data_client_exception_details)
        self.close_connections()
        return list_database_response

    def list_schemas(self):
        required_parameters = [
            "cluster_identifier",
            "database_name",
            "database_user",
            "querying_database"
        ]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            list_schema_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.data_client_status:
                try:
                    schema_list  = []
                    response = self.data_service_client.list_schemas(
                        ClusterIdentifier=self.service_object["cluster_identifier"],
                        ConnectedDatabase=self.service_object["database_name"],
                        Database=self.service_object["querying_database"],
                        DbUser=self.service_object["database_user"]
                    )
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        list_schema_response = common_utils.error_exception_message(self.service_object,
                                                                                     response)
                    else:
                        while 'NextToken' in response:
                            for schema_name in response['Schemas']:
                                schema_list.append(schema_name)
                            response = self.data_service_client.list_schemas(
                                ClusterIdentifier=self.service_object["cluster_identifier"],
                                ConnectedDatabase=self.service_object["database_name"],
                                Database=self.service_object["querying_database"],
                                DbUser=self.service_object["database_user"],
                                NextToken=response['NextToken']
                            )
                        list_schema_response = {
                            "status": True,
                            "schema_count": len(schema_list),
                            "schema_list": schema_list
                        }
                except Exception as exception_details:
                    list_schema_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                list_schema_response = common_utils.error_exception_message(self.service_object,self.data_client_exception_details)
        self.close_connections()
        return list_schema_response

    def list_tables(self):
        required_parameters = [
            "cluster_identifier",
            "database_name",
            "database_user",
            "querying_database",
            "querying_schema"
        ]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            list_table_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.data_client_status:
                try:
                    table_list  = []
                    table_details = {}
                    response = self.data_service_client.list_tables(
                        ClusterIdentifier=self.service_object["cluster_identifier"],
                        ConnectedDatabase=self.service_object["database_name"],
                        Database=self.service_object["querying_database"],
                        DbUser=self.service_object["database_user"],
                        SchemaPattern=self.service_object['querying_schema']
                    )
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        list_table_response = common_utils.error_exception_message(self.service_object,
                                                                                     response)
                    else:
                        while 'NextToken' in response:
                            for table_info in response['Tables']:
                                table_list.append(table_info['name'])
                                table_details[table_info['name']] = {
                                    "schema": table_info['schema'],
                                    "type": table_info['type']
                                }
                            response = self.data_service_client.list_tables(
                                ClusterIdentifier=self.service_object["cluster_identifier"],
                                ConnectedDatabase=self.service_object["database_name"],
                                Database=self.service_object["querying_database"],
                                DbUser=self.service_object["database_user"],
                                SchemaPattern=self.service_object['querying_schema'],
                                NextToken=response['NextToken']
                            )
                        list_table_response = {
                            "status": True,
                            "table_count": len(table_list),
                            "table_list": table_list,
                            "table_details": table_details
                        }
                except Exception as exception_details:
                    list_table_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                list_table_response = common_utils.error_exception_message(self.service_object,self.data_client_exception_details)
        self.close_connections()
        return list_table_response

    def descibe_table(self):
        required_parameters = [
            "cluster_identifier",
            "database_name",
            "database_user",
            "querying_database",
            "querying_schema",
            "querying_table"
        ]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            describe_column_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.data_client_status:
                try:
                    column_list  = []
                    column_details = {}
                    describe_table_paginator = self.data_service_client.get_paginator('describe_table')
                    response = describe_table_paginator.paginate(
                        ClusterIdentifier=self.service_object["cluster_identifier"],
                        ConnectedDatabase=self.service_object["database_name"],
                        Database=self.service_object["querying_database"],
                        DbUser=self.service_object["database_user"],
                        Schema=self.service_object['querying_schema'],
                        Table=self.service_object['querying_table']
                    )
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        describe_column_response = common_utils.error_exception_message(self.service_object,
                                                                                     response)
                    else:
                        for column_info in response['ColumnList']:
                            column_list.append(column_info['name'])
                            column_details[column_info['name']] = {
                                "nullable": column_info['nullable'],
                                "type": column_info['typeName']
                            }
                        describe_column_response = {
                            "status": True,
                            "table_count": len(column_list),
                            "column_list": column_list,
                            "column_details": column_details
                        }
                except Exception as exception_details:
                    describe_column_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                describe_column_response = common_utils.error_exception_message(self.service_object,self.data_client_exception_details)
        self.close_connections()
        return describe_column_response

    def form_super_user_query(self):
        required_parameters = [
            "user_to_check"
        ]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            form_super_user_response = common_utils.missing_key_response(missing_parameters)
        else:
            user_to_check = self.service_object['user_to_check']
            self.service_object['query'] = f"select usesuper from pg_user where usename = '{user_to_check}' and usesuper = 'true'"
            form_super_user_response = {
                "status": True
            }
        return form_super_user_response

    def submit_query(self):
        required_parameters = [
            "cluster_identifier",
            "database_name",
            "database_user",
            "query"
        ]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            submit_query_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.data_client_status:
                try:
                    response = self.data_service_client.execute_statment(
                        Database=self.service_object['database_name'],
                        Sql=self.service_object['query'],
                        ClusterIdentifier=self.service_object['cluster_identifier'],
                        DbUser=self.service_object['database_user'],
                        WithEvent=True
                    )
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        submit_query_response = common_utils.error_exception_message(self.service_object,
                                                                                     response)
                    else:
                        submit_query_response = {
                            "status": True,
                            "query_id": response['Id'].strip('"')
                        }
                except NameError as name_error:
                    submit_query_response = common_utils.error_exception_message(self.service_object,NameError(name_error))
                except Exception as exception_details:
                    submit_query_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                submit_query_response = common_utils.error_exception_message(self.service_object,self.data_client_exception_details)
        return submit_query_response

    def check_query_status(self):
        required_parameters = [
            "query_id"
        ]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            check_query_status_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.data_client_status:
                try:
                    query_completed = False
                    self.reset_time_delay()
                    while not query_completed and self.time_delay <= 6:
                        query_status = self.data_service_client.describe_statement(Id=self.service_object['query_id'])
                        if query_status['ResponseMetadata']['HTTPStatusCode'] != 200:
                            check_query_status_response = common_utils.error_exception_message(self.service_object,
                                                                                         query_status)
                        else:
                            if query_status["Status"] in ['FINISHED','ABORTED','FAILED']:
                                query_completed = True
                                if query_status["Status"] == 'FINISHED':
                                    query_check_status = True
                                else:
                                    query_check_status = False
                            else:
                                time.sleep(2)
                                self.time_delay += 2
                            check_query_status_response = {
                                "status": query_status
                            }
                except Exception as exception_details:
                    check_query_status_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                check_query_status_response = common_utils.error_exception_message(self.service_object,self.data_client_exception_details)
        return check_query_status_response

    def describe_statement(self):
        required_parameters = [
            "query_id"
        ]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            describe_statement_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.data_client_status:
                try:
                    response = self.data_service_client.describe_statement(Id=self.service_object['query_id'])
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        describe_statement_response = common_utils.error_exception_message(self.service_object,
                                                                                           response)
                    else:
                        describe_statement_response = {
                            "status": True,
                            "statement_description": response
                        }
                except WaiterError as waiter_error:
                    describe_statement_response = common_utils.error_exception_message(self.service_object,waiter_error)
                except Exception as exception_details:
                    describe_statement_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                describe_statement_response = common_utils.error_exception_message(self.service_object,self.data_client_exception_details)
        return describe_statement_response

    def get_query_results(self):
        required_parameters = [
            "query_id"
        ]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            query_results_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.data_client_status:
                try:
                    query_results_paginator = self.data_service_client.get_paginator('get_statement_result')
                    query_results_response = {}
                    query_result = []
                    query_data = query_results_paginator.paginate(Id=self.service_object['query_id'])
                    if query_data['ResponseMetadata']['HTTPStatusCode'] != 200:
                        query_results_response = common_utils.error_exception_message(self.service_object,
                                                                                           query_data)
                    else:
                        for data in query_data:
                            query_result.append(data)

                        query_results_response['num_rows'] = query_data[0]["TotalNumRows"]
                        query_results_response['records'] = []
                        for record in query_data[0]["Records"]:
                            query_results_response['records'].append(record)
                        query_results_response['ColumnMetadata'] = query_data[0]["ColumnMetadata"]
                        query_results_response['status'] = True
                        if query_results_response['num_rows'] > 0:
                            query_results_response['data_frame'] = common_utils.get_df(query_results_response['records'],
                                                               query_results_response['ColumnMetadata']
                                                               )
                            query_results_response['returned_by'] = "api"
                except Exception as exception_details:
                    query_results_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            else:
                query_results_response = common_utils.error_exception_message(self.service_object,self.data_client_exception_details)
        return query_results_response

    # end of functions unique to individual services

# Main Entry
def service_api(service_object):
    # start of common code for all services - section 1
    response = {
        "status": False
    }
    cluster_status = ['available']
    if 'region' in service_object:
        service_class = main(service_object['region'])
        service_class.set_service_full_object(service_object)
        service_class.create_service_client_resource()
        if 'action' in service_object:
            # end of common code for all services - section 1
            # start of service specific function calls - section 2
            service_action = service_object['action']
            service_action_response = service_action + "_response"
            if service_class.client_status and service_class.data_client_status:
                if service_action == 'get_clusters':
                    get_cluster_response = service_class.get_clusters(cluster_status)
                    response["status"] = get_cluster_response["status"]
                    response[service_action_response] = get_cluster_response

                elif service_action == 'list_databases':
                    database_response = service_class.list_databases()
                    response["status"] = database_response["status"]
                    response[service_action_response] = database_response

                elif service_action == 'list_schemas':
                    schema_response = service_class.list_schemas()
                    response["status"] = schema_response["status"]
                    response[service_action_response] = schema_response

                elif service_action == 'list_tables':
                    table_response = service_class.list_tables()
                    response["status"] = table_response["status"]
                    response[service_action_response] = table_response

                elif service_action == 'describe_table':
                    describe_table_response = service_class.descibe_table()
                    response["status"] = describe_table_response["status"]
                    response[service_action_response] = describe_table_response

                elif service_action == 'submit_query':
                    submit_query_response = service_class.submit_query()
                    response["status"] = submit_query_response["status"]
                    response[service_action_response] = submit_query_response

                elif service_action == 'check_query_status':
                    query_status_response = service_class.check_query_status()
                    response["status"] = query_status_response["status"]
                    response[service_action_response] = query_status_response

                elif service_action == 'describe_statement':
                    statement_response = service_class.describe_statement()
                    response["status"] = statement_response["status"]
                    response[service_action_response] = statement_response

                elif service_action == 'get_query_results':
                    query_result_response = service_class.get_query_results()
                    response["status"] = query_result_response["status"]
                    response[service_action_response] = query_result_response

                elif service_action in ['submit_and_get_query_results','check_super_user', 'submit_and_check_status']:
                    if service_action == 'check_super_user':
                        service_class.form_super_user_query()
                    submit_query_response = service_class.submit_query()
                    response["status"] = submit_query_response["status"]
                    response[service_action_response] = submit_query_response
                    if submit_query_response["status"]:
                        service_class.set_service_object_key('query_id', submit_query_response['query_id'])
                        query_check_response = service_class.check_query_status()
                        response["status"] = query_check_response["status"]
                        response[service_action_response] = query_check_response
                        if service_action in ['submit_and_get_query_results', 'check_super_user']:
                            if response["status"]:
                                describe_statement_response = service_class.describe_statement()
                                response["status"] = describe_statement_response["status"]
                                response[service_action_response] = describe_statement_response
                                if response["status"]:
                                    query_results_response = service_class.get_query_results()
                                    response["status"] = query_results_response["status"]
                                    response[service_action_response] = query_results_response
                                    if response["status"]:
                                        if service_action == 'check_super_user':
                                            if query_results_response["num_rows"] == 0:
                                                check_query_result_response = {
                                                    "status": False,
                                                    "message": "Given user is not a super user"
                                                }
                                            else:
                                                check_query_result_response = {
                                                    "status": True,
                                                    "message": "Given user is not a super user"
                                                }
                                            response["status"] = check_query_result_response["status"]
                                            response[service_action_response] = check_query_result_response
                                        else:
                                            pass
                                    else:
                                        pass
                                else:
                                    pass
                        else:
                            pass
                    else:
                        if "query_id" in submit_query_response:
                            response['query_id'] = submit_query_response['query_id']
                        else:
                            response['exception_details'] = "Error occurred before query id was generated"
                else:
                    response = common_utils.error_exception_message(service_object,"Invalid Action", inspect.currentframe().f_back.f_lineno)
            else:
                response = common_utils.error_exception_message(service_object,[
                        service_class.data_client_exception_details,
                        service_class.client_exception_details
                    ])
        else:
            response = common_utils.error_exception_message(service_object,"Missing Key 'Action'", inspect.currentframe().f_back.f_lineno)
    else:
        response = common_utils.error_exception_message(service_object,"Missing Key 'region", inspect.currentframe().f_back.f_lineno)
    return response
    # end of service specific function calls - section 3