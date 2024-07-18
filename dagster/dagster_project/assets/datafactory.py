import os
from datetime import datetime, timedelta

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import RunFilterParameters

from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    ResourceParam,
    asset,
)


@asset(
    metadata={"pipeline_name": "trip_data"},
    owners=["kader.hussein.abdi@norges-bank.no"],
)
def raw_trip_data(
    context: AssetExecutionContext,
    adf_client: ResourceParam[DataFactoryManagementClient],
):
    rg_name = "rg-kader"
    df_name = "datafactoryraa5eedxab5ig"
    p_name = "trip_data"

    run_response = adf_client.pipelines.create_run(
        rg_name, df_name, p_name, parameters={"date": "2023-03"}
    )

    pipeline_run = adf_client.pipeline_runs.get(rg_name, df_name, run_response.run_id)
    context.log.info(f"Materializing asset: {pipeline_run.pipeline_name}")

    while True:
        pipeline_run = adf_client.pipeline_runs.get(
            rg_name, df_name, run_response.run_id
        )
        if pipeline_run.status != "InProgress":
            break
    context.log.info(f"\nPipeline run status: {pipeline_run.status}")
    filter_params = RunFilterParameters(
        last_updated_after=datetime.now() - timedelta(1),
        last_updated_before=datetime.now() + timedelta(1),
    )
    query_response = adf_client.activity_runs.query_by_pipeline_run(
        rg_name, df_name, pipeline_run.run_id, filter_params
    )
    activity_run = query_response.value[0]

    if activity_run.status == "Succeeded":
        return MaterializeResult(
            metadata={
                "num_bytes_read": activity_run.output["dataRead"],
                "num_bytes_written": activity_run.output["dataWritten"],
                "copy_duration": activity_run.output["copyDuration"],
                # "preview": MetadataValue.md(df.head().to_markdown()),
            }
        )
    else:
        raise Exception(activity_run.error["message"])
