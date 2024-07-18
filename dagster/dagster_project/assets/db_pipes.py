import sys
from typing import Dict

from dagster import (
    AssetExecutionContext,
    asset,
    open_pipes_session,
)
from dagster_databricks.pipes import (
    PipesDbfsContextInjector,
    PipesDbfsLogReader,
    PipesDbfsMessageReader,
)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs


def custom_databricks_launcher(client: WorkspaceClient, env_vars: Dict[str, str]):
    notebook_path = "/Shared/Sommer2024/DagsterTest"
    cluster_id = "0702-102546-3vc71l2l"
    task = jobs.SubmitTask(
        task_key="some-key",
        existing_cluster_id=cluster_id,
        notebook_task=jobs.NotebookTask(
            notebook_path=notebook_path, base_parameters=env_vars
        ),
    )

    run = client.jobs.submit(run_name="dagster_pipes_job", tasks=[task]).result()
    output = client.jobs.get_run_output(run_id=run.tasks[0].run_id)


@asset
def databricks_pipes(context: AssetExecutionContext):
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
        custom_databricks_launcher(client=client, env_vars=env_vars)

    yield from pipes_session.get_results()
