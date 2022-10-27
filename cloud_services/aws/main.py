import sys
import os.path
libdir = os.path.dirname(__file__)
sys.path.append(os.path.split(libdir)[0])
supported_services = ['s3','dynamodb','redshift','stepfunctions',
                      'athena','lambda','quicksight','secretsmanager']

def cloud_service_api(cloud_service_obj):
    if 'service_name' in cloud_service_obj:
        aws_service_name = cloud_service_obj['service_name'].lower()
        if aws_service_name in supported_services:
            if aws_service_name == 's3':
                from cloud_services.aws.aws_s3 import s3_main
                cloud_service_response = s3_main.service_api(cloud_service_obj)
            elif aws_service_name == 'dynamodb':
                from cloud_services.aws.aws_dynamodb import dynamodb_main
                cloud_service_response = dynamodb_main.service_api(cloud_service_obj)
            elif aws_service_name == 'athena':
                from cloud_services.aws.aws_athena import athena_main
                cloud_service_response = athena_main.service_api(cloud_service_obj)
            elif aws_service_name == 'lambda':
                from cloud_services.aws.aws_lambda import lambda_main
                cloud_service_response = lambda_main.service_api(cloud_service_obj)
            elif aws_service_name == 'stepfunctions':
                from cloud_services.aws.aws_stepfunctions import stepfunction_main
                cloud_service_response = stepfunction_main.service_api(cloud_service_obj)
            elif aws_service_name == 'quicksight':
                from cloud_services.aws.aws_quicksight import quicksight_main
                cloud_service_response = quicksight_main.service_api(cloud_service_obj)
            elif aws_service_name == 'secretsmanager':
                from cloud_services.aws.aws_secretsmanager import secretsmanager_main
                cloud_service_response = secretsmanager_main.service_api(cloud_service_obj)
            elif aws_service_name == 'redshift':
                if 'call_type' in cloud_service_obj:
                    if cloud_service_obj['call_type'] == 'jdbc':
                        from cloud_services.aws.aws_redshift import redshift_jdbc_main
                        cloud_service_response = redshift_jdbc_main.service_api(cloud_service_obj)
                    else:
                        from cloud_services.aws.aws_redshift import redshift_main
                        cloud_service_response = redshift_main.service_api(cloud_service_obj)
        else:
            cloud_service_response = {
                "status": False,
                "error_id": "Unsupported Service",
                "exception_details": f"{cloud_service_obj['service_name']} is not yet supported. Supported Services are {','.join(supported_services)}"
            }
    else:
        cloud_service_response = {
            "status":False,
            "error_id": "Missing Key",
            "exception_details": 'service_name'
        }
    return cloud_service_response