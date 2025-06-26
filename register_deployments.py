# Register Prefect deployments based on pipeline_config.yml
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import yaml
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule, IntervalSchedule

from prefect_flow import pipeline

CONFIG_FILE = Path(__file__).parent / "pipeline_config.yml"


def load_config() -> list[dict]:
    with open(CONFIG_FILE) as f:
        data = yaml.safe_load(f) or {}
    return data.get("sources", [])


def parse_schedule(expr: str):
    s = str(expr).lower()
    if s == "hourly":
        return IntervalSchedule(interval=timedelta(hours=1))
    if s == "daily":
        return IntervalSchedule(interval=timedelta(days=1))
    if s == "weekly":
        return IntervalSchedule(interval=timedelta(weeks=1))
    if s.startswith("every"):
        parts = s.split()
        if len(parts) >= 3:
            interval = int(parts[1])
            unit = parts[2].rstrip("s")
            return IntervalSchedule(interval=timedelta(**{unit + "s": interval}))
        raise ValueError(f"Invalid schedule format: {expr}")
    # Assume HH:MM format for daily execution
    hour, minute = map(int, s.split(":"))
    return CronSchedule(cron=f"{minute} {hour} * * *")


def register_deployments() -> None:
    for source in load_config():
        name = source.get("name", "source")
        fetcher = source["fetcher"]
        models = source.get("models", [])
        schedule = parse_schedule(source.get("schedule", "hourly"))
        deployment = Deployment.build_from_flow(
            flow=pipeline,
            name=name,
            parameters={"fetcher": fetcher, "models": models},
            schedule=schedule,
        )
        deployment.apply()
        print(f"Registered deployment for {name}")


if __name__ == "__main__":
    register_deployments()
