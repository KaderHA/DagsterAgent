from azure.identity import AzureCliCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

import os

adf_client_instance = DataFactoryManagementClient(
    AzureCliCredential(), os.getenv("SUBSCRIPTION_ID")
)
