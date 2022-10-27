import pandas as pd
import io
import os
import inspect
from boto3.s3.transfer import TransferConfig
import json
import logging
from cloud_services import common_utils
from cloud_services.aws.aws_common import connect_to_aws

class main():
    def __init__(self, awsregion):
        self._logger: logging.Logger = logging.getLogger(__name__)
        self.service_name = 's3' #service name
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
            try:
                self.service_client.close()
            except Exception as exception_details:
                print(exception_details)
        if self.sts_client_status:
            try:
                self.sts_client.close()
            except Exception as exception_details:
                print(exception_details)

    # End of common functions

    # start of functions unique to individual services


    def list_buckets(self):
        if self.client_status:
            try:
                bucket_list = []
                bucket_response = self.service_client.list_buckets()
                if bucket_response['ResponseMetadata']['HTTPStatusCode'] != 200:
                    list_bucket_response = common_utils.error_exception_message(self.service_object,
                                                                                 bucket_response)
                else:
                    for bucket_names in bucket_response['Buckets']:
                        bucket_list.append(bucket_names['Name'])
                    list_bucket_response = {
                        "status": True,
                        "bucket_count": len(bucket_list),
                        "bucket_list": bucket_list
                    }
            except Exception as exception_details:
                list_bucket_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
            self.close_connections()
        else:
            list_bucket_response = common_utils.error_exception_message(self.service_object, self.client_exception_details,
                                                                        inspect.currentframe().f_back.f_lineno)
        return list_bucket_response

    def list_objects(self):
        required_parameters = ["bucket_name"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            list_objects_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                try:
                    bucket_contents = []
                    paginator = self.service_client.get_paginator('list_objects_v2')
                    object_response = paginator.paginate(Bucket=self.service_object['bucket_name'])
                    list_objects_response = {
                        "status": False,
                        "page_responses": []
                    }
                    page_status = 0
                    for page in object_response:
                        page_status = page['ResponseMetadata']['HTTPStatusCode']
                    if page_status != 200:
                        list_objects_response = common_utils.error_exception_message(self.service_object,
                                                                                     object_response)
                    else:
                        for page in object_response:
                            if 'Contents' in page:
                                for contents in page['Contents']:
                                    if not (contents is None):
                                        bucket_contents.append(contents['Key'])
                                list_object_page_response = {
                                    "bucket_count": len(bucket_contents),
                                    "bucket_contents": bucket_contents
                                }
                                list_objects_response["page_responses"].append(list_object_page_response)
                        list_objects_response["status"] = True
                except Exception as exception_details:
                    list_objects_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
                self.close_connections()
            else:
                list_objects_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return list_objects_response

    def list_prefix_objects(self):
        required_parameters = ["bucket_name"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            list_prefix_objects_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                try:
                    bucket_contents = []
                    files = []
                    folders = []
                    folder_files = {}
                    paginator = self.service_client.get_paginator('list_objects_v2')
                    object_response = paginator.paginate(Bucket=self.service_object['bucket_name'])
                    page_status = 0
                    for page in object_response:
                        page_status = page['ResponseMetadata']['HTTPStatusCode']
                    if page_status != 200:
                        list_prefix_objects_response = common_utils.error_exception_message(self.service_object,
                                                                                     object_response)
                    else:
                        list_prefix_objects_response = {
                            "status": False,
                            "page_responses": []
                        }
                        for page in object_response:
                            for contents in page['Contents']:
                                if not (contents is None):
                                    bucket_contents.append(contents['Key'])

                            unique_prefix_contents = common_utils.remove_duplicates(bucket_contents)

                            for contents in unique_prefix_contents:
                                level_count = contents.count("/")
                                if level_count == 0:
                                    files.append(contents)
                                else:
                                    paths = contents.split("/")
                                    folders.append(paths[0])
                                    if not (paths[0] in folder_files):
                                        folder_files[paths[0]] = []
                                    if not (paths[1:-1] in folder_files[paths[0]]):
                                        folder_files[paths[0]].append(paths[1:-1])

                            unique_folders = common_utils.remove_duplicates(folders)
                            list_prefix_objects_page_response = {
                                "files": files,
                                "file_count": len(files),
                                "folders": unique_folders,
                                "folder_count": len(unique_folders)
                            }
                            list_prefix_objects_response['page_responses'].append(list_prefix_objects_page_response)
                        list_prefix_objects_response['status'] = True
                except Exception as exception_details:
                    list_prefix_objects_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
                self.close_connections()
            else:
                list_prefix_objects_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return list_prefix_objects_response

    def list_objects_in_prefix(self):
        required_parameters = ["bucket_name","prefix_name"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            list_objects_in_prefix_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.client_status:
                try:
                    bucket_contents = []
                    paginator = self.service_client.get_paginator('list_objects_v2')
                    object_response = paginator.paginate(
                        Bucket=self.service_object['bucket_name'],
                        Prefix=self.service_object['prefix_name']
                    )
                    page_status = 0
                    for page in object_response:
                        page_status = page['ResponseMetadata']['HTTPStatusCode']
                    if page_status != 200:
                        list_objects_in_prefix_response = common_utils.error_exception_message(self.service_object,
                                                                                     object_response)
                    else:
                        list_objects_in_prefix_response = {
                            "status": False,
                            "page_responses": []
                        }
                        for page in object_response:
                            for contents in page['Contents']:
                                prefix_contents = contents['Key']
                                if not (contents is None):
                                    prefix_parts = prefix_contents.split("/")
                                    object_name_prefix = "/".join(prefix_parts[0:-1])
                                    object_name = prefix_parts[-1]
                                    if object_name_prefix == self.service_object['prefix_name']:
                                        bucket_contents.append(object_name)

                            unique_prefix_contents = common_utils.remove_duplicates(bucket_contents)

                            list_objects_in_prefix_page_response = {
                                "prefix_contents": unique_prefix_contents,
                                "prefix_object_count": len(unique_prefix_contents)
                            }
                            list_objects_in_prefix_response['page_responses'].append(list_objects_in_prefix_page_response)
                        list_objects_in_prefix_page_response['status'] = True
                except Exception as exception_details:
                    list_objects_in_prefix_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
                self.close_connections()
            else:
                list_objects_in_prefix_response = common_utils.error_exception_message(self.service_object,self.client_exception_details)
        return list_objects_in_prefix_response

    def download_object_from_s3(self):
        required_parameters = ["bucket_name","prefix_name"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            download_object_from_s3_response = common_utils.missing_key_response(missing_parameters)
        else:
            if "local_file_name" not in self.service_object:
                self.service_object["local_file_name"] = self.service_object["prefix_name"].split("/")[-1]
            if self.resource_status:
                try:
                    response = self.service_resource.Bucket(
                        self.service_object['bucket_name']).download_file(
                        self.service_object['prefix_name'],
                        self.service_object['local_file_name']
                    )
                    download_object_from_s3_response = {
                        "status": True
                    }
                except Exception as exception_details:
                    download_object_from_s3_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
                self.close_connections()
            else:
                download_object_from_s3_response = common_utils.error_exception_message(self.service_object, self.resource_exception_details)
        return download_object_from_s3_response

    def upload_object_to_s3(self):
        required_parameters = ["bucket_name","prefix_name","local_file_name","delete_flag"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            upload_object_to_s3_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.resource_status:
                try:
                    response = self.service_resource.Bucket(
                        self.service_object['bucket_name']).upload_file(
                        self.service_object['local_file_name'],
                        self.service_object['prefix_name']
                    )
                    upload_object_to_s3_response = {
                        "status": True
                    }
                    if self.service_object['delete_flag']:
                        try:
                            os.remove(self.service_object['local_file_name'])
                            upload_object_to_s3_response['local_file_delete_status'] = True
                        except Exception as exception_details:
                            upload_object_to_s3_response['local_file_delete_status'] = False
                            upload_object_to_s3_response['local_file_delete_message'] = exception_details
                except Exception as exception_details:
                    upload_object_to_s3_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
                self.close_connections()
            else:
                upload_object_to_s3_response = common_utils.error_exception_message(self.service_object,
                                                                                       self.resource_exception_details)
        return upload_object_to_s3_response

    def read_file_in_s3(self):
        required_parameters = ["bucket_name","prefix_name","file_type"]
        missing_parameters = common_utils.check_missing_parms(self.service_object, required_parameters)
        if missing_parameters["status"]:
            read_file_in_s3_response = common_utils.missing_key_response(missing_parameters)
        else:
            if self.service_object['file_type'].lower() == 'parquet':
                read_file_in_s3_response = self.read_parquet_file_in_s3()
            elif self.service_object['file_type'].lower() == 'json':
                read_file_in_s3_response = self.read_json_file_in_s3()
            elif self.service_object['file_type'].lower() == 'csv':
                read_file_in_s3_response = self.read_csv_file_in_s3()
            elif self.service_object['file_type'].lower() in ['txt','text']:
                read_file_in_s3_response = self.read_text_file_in_s3()
            else:
                read_file_in_s3_response = common_utils.error_exception_message(self.service_object,f"Only {','.join(common_utils.supported_file_types)} file types are supported")
        return read_file_in_s3_response

    def read_text_file_in_s3(self):
        try:
            if self.resource_status:
                object_content = self.service_resource.Object(self.service_object['bucket_name'],self.service_object['prefix_name'])
                response = object_content.get()
                if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                    read_text_file_in_s3_response = common_utils.error_exception_message(self.service_object,response)
                else:
                    response_data = response['Body'].read().decode('utf-8')
                    line_string_words = response_data.split(" ")
                    text_array = []
                    text_dict = {}
                    text_details = []
                    for words in line_string_words:
                        cleaned_words = words.replace('\r\n\r\n',' ')
                        text_array.append(cleaned_words)
                        if cleaned_words not in text_dict:
                            text_dict[cleaned_words] = 1
                        else:
                            count_word = text_dict[cleaned_words]
                            text_dict[cleaned_words] = count_word + 1

                    text_details.append(f"Number of words in the text is {str(len(text_array))}")
                    unique_text_array = common_utils.remove_duplicates(text_array)
                    text_details.append(f"Number of unique words in the text is {str(len(unique_text_array))}")
                    sort_text_dict = sorted(text_dict.items(), key=lambda x: x[1], reverse=True)
                    for word, count in sort_text_dict:
                        text_details.append(f"The word {str(word)} appears {str(count)} times")

                    data_frame = pd.DataFrame(text_details)
                    if data_frame.empty:
                        read_text_file_in_s3_response = {
                            "status": True,
                            "number_of_rows": 0
                        }
                    else:
                        data_frame.columns = ['Text File Details']
                        read_text_file_in_s3_response = {
                            "status": True,
                            "data": data_frame.to_dict('records'),
                            "columns": list(data_frame.columns),
                            "number_of_rows": data_frame.shape[0]
                        }
            else:
                read_text_file_in_s3_response = common_utils.error_exception_message(self.service_object,
                                                                                        self.client_exception_details,
                                                                                        inspect.currentframe().f_back.f_lineno)
        except Exception as exception_details:
            read_text_file_in_s3_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
        return read_text_file_in_s3_response

    def read_csv_file_in_s3(self):
        try:
            if self.client_status:
                object_content = self.service_client.get_object(Bucket=self.service_object['bucket_name'],Key=self.service_object['prefix_name'])
                data_frame = pd.read_csv(io.BytesIO(object_content['Body'].read()), on_bad_lines='warn')
                data_frame = data_frame.fillna(method='bfill')
                if data_frame.empty:
                    read_csv_file_in_s3_response = {
                        "status": True,
                        "number_of_rows": 0
                    }
                else:
                    read_csv_file_in_s3_response = {
                        "status": True,
                        "data": data_frame.to_dict('records'),
                        "columns": list(data_frame.columns),
                        "number_of_rows": data_frame.shape[0]
                    }
                self.close_connections()
            else:
                read_csv_file_in_s3_response = common_utils.error_exception_message(self.service_object,
                                                                                        self.client_exception_details,
                                                                                        inspect.currentframe().f_back.f_lineno)
        except UnicodeDecodeError as encoding_error:
            read_csv_file_in_s3_response = common_utils.error_exception_message(self.service_object, encoding_error)
        except Exception as exception_details:
            read_csv_file_in_s3_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
        return read_csv_file_in_s3_response

    def read_json_file_in_s3(self):
        try:
            if self.resource_status:
                object_content = self.service_resource.Object(self.service_object['bucket_name'],self.service_object['prefix_name'])
                response = object_content.get()
                if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                    read_json_file_in_s3_response = common_utils.error_exception_message(self.service_object,
                                                                                         response)
                else:
                    response_data = response['Body'].read().decode('utf-8')
                    response_data = response_data.replace('}{','},{')
                    response_data = response_data.replace("'",'"')
                    response_data = '{"data":['+response_data+']}'
                    try:
                        json_content = json.loads(response_data)
                    except:
                        json_content = [json.loads(line) for line in response_data]
                    if json_content:
                        data_frame = pd.DataFrame.from_dict(json_content['data'])
                        if data_frame.empty:
                            read_json_file_in_s3_response = {
                                "status": True,
                                "number_of_rows": 0
                            }
                        else:
                            read_json_file_in_s3_response = {
                                "status": True,
                                "data": data_frame.to_dict('records'),
                                "columns": list(data_frame.columns),
                                "number_of_rows": data_frame.shape[0]
                            }
                    else:
                        read_json_file_in_s3_response = {
                            "status": True,
                            "number_of_rows": 0
                        }
            else:
                read_json_file_in_s3_response = common_utils.error_exception_message(self.service_object,
                                                                                        self.resource_exception_details,
                                                                                        inspect.currentframe().f_back.f_lineno)
        except Exception as exception_details:
            read_json_file_in_s3_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
        return read_json_file_in_s3_response

    def read_parquet_file_in_s3(self):
        try:
            if self.resource_status:
                transfer_configuration = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,multipart_chunksize=1024 * 25, use_threads=True)
                buffer = io.BytesIO()
                bucket_object = self.service_resource.Object(self.service_object['bucket_name'],self.service_object['prefix_name'])
                bucket_object.download_fileobj(buffer, Config=transfer_configuration)
                data_frame = pd.read_parquet(buffer)
                if data_frame.empty:
                    read_parquet_file_in_s3_response = {
                        "status": True,
                        "number_of_rows": 0
                    }
                else:
                    read_parquet_file_in_s3_response = {
                        "status": True,
                        "data": data_frame.to_dict('records'),
                        "columns": list(data_frame.columns),
                        "number_of_rows": data_frame.shape[0]
                    }
            else:
                read_parquet_file_in_s3_response = common_utils.error_exception_message(self.service_object,
                                                                                        self.resource_exception_details,
                                                                                        inspect.currentframe().f_back.f_lineno)
        except Exception as exception_details:
            read_parquet_file_in_s3_response = common_utils.error_exception_message(self.service_object, exception_details, inspect.currentframe().f_back.f_lineno)
        return read_parquet_file_in_s3_response
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
            if service_action == 'get_buckets':
                object_response = service_class.list_buckets()
                response["status"] = object_response["status"]
                response[service_action_response] = object_response
            elif service_action == 'get_object_list':
                object_response = service_class.list_objects()
                response["status"] = object_response["status"]
                response[service_action_response] = object_response
            elif service_action == 'get_prefix_objects':
                object_response = service_class.list_prefix_objects()
                response["status"] = object_response["status"]
                response[service_action_response] = object_response
            elif service_action == 'get_objects_in_prefix':
                object_response = service_class.list_objects_in_prefix()
                response["status"] = object_response["status"]
                response[service_action_response] = object_response
            elif service_action == 'download_object':
                object_response = service_class.download_object_from_s3()
                response["status"] = object_response["status"]
                response[service_action_response] = object_response
            elif service_action == 'upload_object':
                object_response = service_class.upload_object_to_s3()
                response["status"] = object_response["status"]
                response[service_action_response] = object_response
            elif service_action == 'read_s3_file':
                object_response = service_class.read_file_in_s3()
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