import setuptools
import sys
from cloud_services import version

version = version.version
name = None
if '--version' in sys.argv:
    index = sys.argv.index('--version')
    sys.argv.pop(index)
    version = sys.argv.pop(index)

if '--name' in sys.argv:
    index = sys.argv.index('--name')
    sys.argv.pop(index)
    version = sys.argv.pop(index)

with open("Readme.md","r") as fh:
    long_description = fh.read()

setuptools.setup(
    name=name,
    version=version,
    author="Gen 3 Learning",
    autor_email="techsupport@gen3learning.com",
    description="API to call different cloud providers services",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=["cloud_services","cloud_services/aws","cloud_services/gcp","cloud_services/azure"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operationg System :: OS Independent",
    ],
)
