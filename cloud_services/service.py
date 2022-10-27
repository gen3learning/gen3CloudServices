import sys
import os.path
libdir = os.path.dirname(__file__)
sys.path.append(os.path.split(libdir)[0])
from cloud_services import version

print('Using Gen3 Cloud Services Version', version.version)

supported_providers = ['aws']

def cloud_service_api(cloud_service_obj):
    if 'cloud_provider_name' in cloud_service_obj:
        if cloud_service_obj['cloud_provider_name'] in supported_providers:
            if cloud_service_obj['cloud_provider_name'] == 'aws':
                from cloud_services.aws import main
                cloud_service_response = main.cloud_service_api(cloud_service_obj)
        else:
            cloud_service_response = {
                "status": False,
                "error_id": "Unsupported Provider",
                "exception_details": f"{cloud_service_obj['cloud_provider_name']} is not yet supported. Supported Providers are {','.join(supported_providers)}"
            }
    else:
        cloud_service_response = {
            "status":False,
            "error_id": "Missing Key",
            "exception_details": 'cloud_provider_name'
        }
    return cloud_service_response