import logging
import boto3
from cloud_services import common_utils

class aws_credentials:
    aws_access_key = ''
    aws_secret_access_key = ''
    aws_session_token = ''
    aws_account_id = ''
    aws_user_arn = ''
    aws_user = ''
    credentials_response = {}
    current_session = None
    sts_client = None

    def __init__(self):
        self._logger: logging.Logger = logging.getLogger(__name__)

    def get_credentials(self, credentials_object, assume_role=False):
        try:
            aws_credentials.sts_client = boto3.client('sts',credentials_object['region'])
            aws_credentials.aws_account_id = aws_credentials.sts_client.get_caller_identity()["Account"]
            aws_credentials.aws_user_arn = aws_credentials.sts_client.get_caller_identity()["Arn"]
            if aws_credentials.aws_user_arn.count("user") > 0:
                aws_credentials.aws_user = aws_credentials.aws_user_arn.split("/")[-1]
            if 'account_id' in credentials_object:
                if aws_credentials.aws_account_id != credentials_object["account_id"]:
                    required_parameters = ["role"]
                    missing_parameters = common_utils.check_missing_parms(credentials_object, required_parameters)
                    if missing_parameters["status"]:
                        self.credentials_response = common_utils.missing_key_response(missing_parameters)
                    else:
                        try:
                            role_to_assume = 'arn:aws:iam::' + credentials_object["account_id"] + ":role/" + \
                                             credentials_object["role"]
                            session_name = "cross_account_access_" + aws_credentials.aws_account_id
                            aws_credentials.current_session = aws_credentials.sts_client.assume_role(
                                RoleArn=role_to_assume,
                                RoleSessionName=session_name
                            )
                            aws_credentials.aws_access_key = aws_credentials.current_session['Credentials']['AccessKeyId']
                            aws_credentials.aws_secret_access_key = aws_credentials.current_session['Credentials']['SecretAccessKey']
                            aws_credentials.aws_session_token = aws_credentials.current_session['Credentials']['SessionToken']
                            aws_credentials.credentials_response['status'] = True
                        except Exception as exception_details:
                            aws_credentials.credentials_response['status'] = False
                            aws_credentials.credentials_response['error_id'] = f"Error occurred while assuming role with account {credentials_object['account_id']}"
                            aws_credentials.credentials_response["exception_details"] = exception_details
                else:
                    self.get_current_account_credentials(credentials_object)
            else:
                if not assume_role:
                    self.get_current_account_credentials(credentials_object)
        except Exception as exception_details:
            self.cred_status = False
            self.cred_status_message = "Unable to set sts client"
            self.cred_exception_details = exception_details

    def get_current_account_credentials(self, credentials_object):
        if "profile" in credentials_object:
            try:
                aws_credentials.current_session = boto3.session.Session(
                    region_name=credentials_object["region"],
                    profile_name=credentials_object['profile']
                )
                session_status = True
            except Exception as exception_details:
                session_status = False
                exception_details_message = exception_details
        else:
            try:
                aws_credentials.current_session = boto3.session.Session(region_name=credentials_object["region"])
                session_status = True
            except Exception as exception_details:
                session_status = False
                exception_details_message = exception_details
        if session_status:
            current_credentials = aws_credentials.current_session.get_credentials()
            same_account_credentials = current_credentials.get_frozen_credentials()
            aws_credentials.aws_access_key = same_account_credentials.access_key
            aws_credentials.aws_secret_access_key = same_account_credentials.secret_key
            aws_credentials.aws_session_token = same_account_credentials.token
            aws_credentials.credentials_response['status'] = True
        else:
            aws_credentials.credentials_response['status'] = False
            aws_credentials.credentials_response['error_id'] = "Unable to set session with same account"
            aws_credentials.credentials_response['exception_details'] = exception_details_message