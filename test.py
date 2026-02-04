import os
import shutil
from datetime import timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def cleanup_logs_except_dag_processor(**kwargs):
    log_folder = "/opt/airflow/logs"  # Your path
    excluded_folder = "dag_processor"  # Exact folder name to preserve

    if not os.path.exists(log_folder):
        print(f"Log folder not found: {log_folder}")
        return

    deleted_count = 0
    skipped_count = 0

    def cleanup_dag_processor_folder(dag_processor_path):
        """Clean up dag_processor folder, keeping only 'latest' and most recent date folder"""
        if not os.path.exists(dag_processor_path):
            return 0

        items = os.listdir(dag_processor_path)
        date_folders = []

        # Identify date folders (format: YYYY-MM-DD)
        for item in items:
            item_path = os.path.join(dag_processor_path, item)
            if os.path.isdir(item_path) and item != "latest":
                # Check if it looks like a date folder
                if len(item) == 10 and item.count("-") == 2:
                    try:
                        # Validate it's a proper date
                        parts = item.split("-")
                        if (
                            len(parts[0]) == 4
                            and len(parts[1]) == 2
                            and len(parts[2]) == 2
                        ):
                            date_folders.append(item)
                    except:
                        pass

        # Sort date folders to find the most recent
        date_folders.sort(reverse=True)
        most_recent_date = date_folders[0] if date_folders else None

        # Delete everything except 'latest' and most recent date folder
        deleted = 0
        for item in items:
            item_path = os.path.join(dag_processor_path, item)
            if item == "latest":
                print(f"Keeping 'latest' folder: {item_path}")
                continue
            if item == most_recent_date:
                print(f"Keeping most recent date folder: {item_path}")
                continue

            try:
                if os.path.isfile(item_path):
                    os.remove(item_path)
                    print(f"Deleted file in dag_processor: {item_path}")
                    deleted += 1
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                    print(f"Deleted old date folder in dag_processor: {item_path}")
                    deleted += 1
            except OSError as e:
                print(f"Error deleting {item_path}: {e}")

        return deleted

    for item in os.listdir(log_folder):
        item_path = os.path.join(log_folder, item)
        item_name = os.path.basename(item_path)
        if item_name == excluded_folder:
            print(f"Cleaning up dag_processor folder: {item_path}")
            deleted_count += cleanup_dag_processor_folder(item_path)
            skipped_count += 1
            continue
        try:
            if os.path.isfile(item_path):
                os.remove(item_path)
                print(f"Deleted file: {item_path}")
                deleted_count += 1
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
                print(f"Deleted directory: {item_path}")
                deleted_count += 1
        except OSError as e:
            print(f"Error deleting {item_path}: {e}")
    print(
        f"Cleanup complete: deleted {deleted_count} items, preserved {skipped_count} (dag_processor)"
    )


with DAG(
    dag_id="log_cleanup_aggressive_except_dag_processor",
    default_args=default_args,
    description="Delete ALL logs except dag_processor folder",
    schedule="*/5 * * * *",  # Run daily; change to '@hourly' if you want even more aggressive
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    tags=["maintenance"],
) as dag:
    cleanup_task = PythonOperator(
        task_id="delete_all_except_dag_processor",
        python_callable=cleanup_logs_except_dag_processor,
    )
    cleanup_task
