import sys
from typing import Dict

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetExecutionContext,
    asset,
    asset_check,
    open_pipes_session,
)
from dagster._core.pipes.context import PipesExecutionResult
from dagster_databricks.pipes import (
    PipesDbfsContextInjector,
    PipesDbfsLogReader,
    PipesDbfsMessageReader,
)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs


def asset_setup(client: WorkspaceClient, env_vars: Dict[str, str]):
    notebook_path = "/Users/kader.hussein.abdi@norges-bank.no/a_trips_data"
    cluster_id = "0719-111925-uevx8vrc"
    task = jobs.SubmitTask(
        task_key="some-key",
        existing_cluster_id=cluster_id,
        notebook_task=jobs.NotebookTask(
            notebook_path=notebook_path, base_parameters=env_vars
        ),
    )

    run = client.jobs.submit(run_name="dagster_pipes_job", tasks=[task]).result()
    output = client.jobs.get_run_output(run_id=run.tasks[0].run_id)


def asset_check_setup_gx(client: WorkspaceClient, env_vars: Dict[str, str]):
    notebook_path = "/Users/kader.hussein.abdi@norges-bank.no/ac_trips_data_no_nulls_gx"
    cluster_id = "0719-111925-uevx8vrc"
    task = jobs.SubmitTask(
        task_key="some-key",
        existing_cluster_id=cluster_id,
        notebook_task=jobs.NotebookTask(
            notebook_path=notebook_path, base_parameters=env_vars
        ),
    )
    run = client.jobs.submit(run_name="dagster_pipes_job", tasks=[task]).result()


def asset_check_setup_soda(client: WorkspaceClient, env_vars: Dict[str, str]):
    notebook_path = (
        "/Users/kader.hussein.abdi@norges-bank.no/ac_trips_data_no_nulls_soda"
    )
    cluster_id = "0719-111925-uevx8vrc"
    task = jobs.SubmitTask(
        task_key="some-key",
        existing_cluster_id=cluster_id,
        notebook_task=jobs.NotebookTask(
            notebook_path=notebook_path, base_parameters=env_vars
        ),
    )
    run = client.jobs.submit(run_name="dagster_pipes_job", tasks=[task]).result()


@asset
def databricks_asset(context: AssetExecutionContext):
    client = WorkspaceClient()

    with open_pipes_session(
        context=context,
        extras={"foo": "bar"},
        context_injector=PipesDbfsContextInjector(client=client),
        message_reader=PipesDbfsMessageReader(
            client=client,
            log_readers=[
                PipesDbfsLogReader(
                    client=client, remote_log_name="stdout", target_stream=sys.stdout
                ),
                PipesDbfsLogReader(
                    client=client, remote_log_name="stderr", target_stream=sys.stderr
                ),
            ],
        ),
    ) as pipes_session:
        env_vars = pipes_session.get_bootstrap_env_vars()
        asset_setup(client=client, env_vars=env_vars)

    yield from pipes_session.get_results()


@asset_check(asset=databricks_asset)
def databricks_asset_check_gx(context: AssetCheckExecutionContext):
    client = WorkspaceClient()

    with open_pipes_session(
        context=context._op_execution_context,
        extras={"foo": "bar"},
        context_injector=PipesDbfsContextInjector(client=client),
        message_reader=PipesDbfsMessageReader(
            client=client,
            log_readers=[
                PipesDbfsLogReader(
                    client=client, remote_log_name="stdout", target_stream=sys.stdout
                ),
                PipesDbfsLogReader(
                    client=client, remote_log_name="stderr", target_stream=sys.stderr
                ),
            ],
        ),
    ) as pipes_session:
        env_vars = pipes_session.get_bootstrap_env_vars()
        asset_check_setup_gx(client=client, env_vars=env_vars)

    yield from pipes_session.get_results()


@asset_check(asset=databricks_asset)
def databricks_asset_check_soda(context: AssetCheckExecutionContext):
    client = WorkspaceClient()

    with open_pipes_session(
        context=context._op_execution_context,
        extras={"foo": "bar"},
        context_injector=PipesDbfsContextInjector(client=client),
        message_reader=PipesDbfsMessageReader(
            client=client,
            log_readers=[
                PipesDbfsLogReader(
                    client=client, remote_log_name="stdout", target_stream=sys.stdout
                ),
                PipesDbfsLogReader(
                    client=client, remote_log_name="stderr", target_stream=sys.stderr
                ),
            ],
        ),
    ) as pipes_session:
        env_vars = pipes_session.get_bootstrap_env_vars()
        asset_check_setup_soda(client=client, env_vars=env_vars)

    yield from pipes_session.get_results()
