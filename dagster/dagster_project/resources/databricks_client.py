from dagster_databricks import PipesDatabricksClient, databricks_client
from databricks.sdk import WorkspaceClient
import os

databricks_client_instance = databricks_client.configured(
    {
        "host": {"env": "DATABRICKS_HOST"},
        "token": {"env": "DATABRICKS_TOKEN"},
    }
)

pipes_databricks_resource = PipesDatabricksClient(
    client=WorkspaceClient(
        token=str(os.getenv("DATABRICKS_TOKEN")),
        host=str(os.getenv("DATABRICKS_HOST")),
    )
)
