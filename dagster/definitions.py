import os
import shutil
from pathlib import Path

import dagster as dg

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


@dg.asset(
    group_name="sporttery_ingestion",
    kinds={"rust", "http"},
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
)
def sporttery_bronze(
    context: dg.AssetExecutionContext,
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.MaterializeResult:
    if not os.environ.get("SPORTTERY_ODDS_URL"):
        raise dg.Failure("SPORTTERY_ODDS_URL must be configured on the household-network host")

    cargo = shutil.which("cargo")
    if cargo is None:
        raise dg.Failure("cargo must be available to the Dagster process")
    resource = os.environ.get("SPORTTERY_RESOURCE", "sporttery_odds")
    output = os.environ.get("FOOTBALL_DATA_ROOT", str(REPOSITORY_ROOT / "data" / "bronze"))
    command = [
        cargo,
        "run",
        "--release",
        "-p",
        "football-ingest",
        "--",
        "sporttery",
        "--resource",
        resource,
        "--output",
        output,
    ]
    return pipes_subprocess_client.run(
        command=command,
        cwd=str(REPOSITORY_ROOT),
        context=context,
    ).get_materialize_result()


defs = dg.Definitions(
    assets=[sporttery_bronze],
    schedules=[
        dg.ScheduleDefinition(
            job=dg.define_asset_job(
                "sporttery_capture_job",
                selection=dg.AssetSelection.assets(sporttery_bronze),
            ),
            cron_schedule=os.environ.get("SPORTTERY_CRON", "0 8 * * *"),
            execution_timezone=os.environ.get("SPORTTERY_TIMEZONE", "Asia/Shanghai"),
        )
    ],
    resources={
        "pipes_subprocess_client": dg.PipesSubprocessClient(
            context_injector=dg.PipesEnvContextInjector(),
        )
    },
)
