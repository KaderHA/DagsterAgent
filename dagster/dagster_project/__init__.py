from dagster import Definitions, ScheduleDefinition, load_assets_from_modules

from .assets import dagster_databricks_pipes, db_pipes
from .resources import databricks_client

all_assets = load_assets_from_modules([dagster_databricks_pipes, db_pipes])

defs = Definitions(
    assets=all_assets,
    asset_checks=[
        db_pipes.databricks_asset_check_gx,
        db_pipes.databricks_asset_check_soda,
    ],
    resources={
        "databricks": databricks_client.databricks_client_instance,
        "pipes_databricks": databricks_client.pipes_databricks_resource,
    },
)
