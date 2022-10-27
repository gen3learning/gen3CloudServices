import pandas as pd
import inspect
import itertools
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.lib as pl
import os
import json

supported_file_types = ['json','parquet','txt','text','csv']

def check_missing_parms(input_object, parm_list):
    missing_parameters_list = []
    for parms in parm_list:
        if parms not in input_object:
            missing_parameters_list.append(parms)

    if len(missing_parameters_list) > 0:
        missing_parm_response = {
            "status": True,
            "count": len(missing_parameters_list),
            "parameters": missing_parameters_list
        }
    else:
        missing_parm_response = {
            "status": False
        }
    return missing_parm_response

def missing_key_response(missing_parms):
    called_function_info = inspect.currentframe().f_back
    function_name = called_function_info.f_code.co_name
    line_number = called_function_info.f_lineno
    missing_key_response = {
        "status": False,
        "error_function_name": f"Error in function: {function_name} at line number {str(line_number)}",
        "error_message": "Missing Keys",
        "missing_key_count": missing_parms['count'],
        "missing_keys": ','.join(missing_parms['parameters'])
    }
    return missing_key_response

def get_df(records, column_metadata):
    columns = []
    for col_details in column_metadata:
        columns.append(col_details["name"])
    rows = []
    for row in records:
        temp_array = []
        for fields in row:
            temp_array.append(fields[list(fields.keys())[0]])
        rows.append(temp_array)
        df = pd.DataFrame(rows,columns=columns).to_json()
        return df

def get_dataframe_from_json(json_data,data_fields):
    data = json.loads(json_data)
    data_frame = [data.get(key) for key in data_fields]
    return data_frame

def error_exception_message(
        service_object,
        exception_error_details,
        exception_line_number):
    called_function_info = inspect.currentframe().f_back
    function_name = called_function_info.f_code.co_name
    line_number = called_function_info.f_lineno
    error_exception_response = {
        "status": False,
        "cloud_provider_name": service_object["cloud_provider_name"],
        "service_name": service_object["service_name"],
        "action_name": service_object["action"],
        "error_message": f"Error occured in function:{function_name} line number {str(line_number)}",
        "error_or_exception_details": exception_error_details,
        "error_line_number": exception_line_number
    }
    return error_exception_response

def remove_duplicates(input_list):
    unique_value_list = []
    for values in input_list:
        if values not in unique_value_list:
            unique_value_list.append(values)
    return unique_value_list

def get_spark_df_from_csv(service_object, csv_data,spark=None):
    if spark:
        data_response = pandas_to_spark(service_object, csv_data)
        if data_response["status"] and data_response['data_exists']:
            spark_df = data_response["data"]
            number_or_rows = spark_df.count()
            if number_or_rows > 0:
                get_spark_df_from_csv_response = load_field_dictionary(spark_df)
                get_spark_df_from_csv_response['num_rows'] = number_or_rows
            else:
                get_spark_df_from_csv_response = error_exception_message(service_object, "no data returned")
        else:
            get_spark_df_from_csv_response = data_response
    return get_spark_df_from_csv_response

def pandas_to_spark(service_object, panda_input):
    if panda_input.shape[0] == 0:
        pandas_to_spark_response = {
            "status": True,
            "data_exists": False,
            "message": "No data in the input"
        }
    else:
        exclusive_columns = panda_input.columns[~panda_input.columns.duplicated(keep=False)]
        increment_value = itertools.count().__next__
        def rename(name):
            return f"{name}{increment_value()}" if name not in exclusive_columns else name
        panda_input = panda_input.rename(columns=rename)
        try:
            panda_table = pa.Table.from_pandas(panda_input)
            pq.write_table(panda_table, os.path.dirname((os.getcwd())) + "/temp/temp.parquet")
        except pl.ArrowTypeError as arrow_error:
            pandas_to_spark_response = error_exception_message(service_object, arrow_error)
        except TypeError as type_error:
            pandas_to_spark_response = error_exception_message(service_object, type_error)
        except ValueError as value_error:
            pandas_to_spark_response = error_exception_message(service_object, value_error)
        except Exception as exception_details:
            pandas_to_spark_response = error_exception_message(service_object, exception_details)
    return pandas_to_spark_response

def load_field_dictionary(spark_dataframe):
    field_dictionary = {}
    for key, value in spark_dataframe.dtypes:
        field_dictionary.setdefault(key,[]).append(value)
    cols = spark_dataframe.columns
    cols.sort()
    field_types = spark_dataframe.dtypes
    field_dictionary_response = {
        "status": True,
        "data_exists": True,
        "field_dictionary": field_dictionary,
        "column_info": cols,
        "field_types": field_types,
        "data": spark_dataframe
    }
    return field_dictionary_response

def search_json_objects(json_input,search_key, search_value):
    search_output = {
        "status": False,
        "data": []
    }
    for data in json_input:
        if data[search_key] == search_value:
            search_output['data'].append(data)
    if len(search_output['data']) > 0:
        search_output["status"] = True
    return search_output

def get_bucket_name_from_s3_path(s3_full_path,path_contains_object):
    if s3_full_path[0:5] == "s3://":
        output_location = s3_full_path[5:].split("/")
    else:
        output_location = s3_full_path.split("/")

    bucket_name = output_location[0]
    contacted_prefix = []

    for prefix in output_location[1:]:
        if len(prefix) > 0:
            contacted_prefix.append(prefix)

    prefix = "/".join(contacted_prefix)

    full_path = "s3://" + bucket_name + "/" + prefix

    if not path_contains_object:
        full_path = full_path + "/"

    s3_bucket_parts = {
        "bucket_name": bucket_name,
        "prefix": prefix,
        "full_path": full_path
    }
    return s3_bucket_parts