import subprocess
import schedule
import time


def run_dbt_pipeline():
    """Run dbt commands for the full pipeline."""
    subprocess.run(["dbt", "seed"], check=True)
    subprocess.run(["dbt", "run"], check=True)
    subprocess.run(["dbt", "test"], check=True)


def main():
    schedule.every().day.at("00:00").do(run_dbt_pipeline)
    print("Starting orchestration loop. Press Ctrl+C to stop.")
    run_dbt_pipeline()
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
