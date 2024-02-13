import os
from pathlib import Path

from typing import Dict, List, Tuple
from datetime import datetime
import csv
import zipfile

import paramiko
from paramiko.sftp_client import SFTPClient
from psycopg2.extras import execute_batch
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


LOCAL_DIRECTORY: Path = Path(__file__).parent.parent.resolve()
REMOTE_DIRECTORY: str = Variable.get("REMOTE_DIRECTORY")
HOSTNAME_FTP: str = Variable.get("HOSTNAME_FTP")
PORT_FTP: int = int(Variable.get("PORT_FTP"))
USERNAME_FTP: str = Variable.get("USERNAME_FTP")
PASSWORD_FTP: str = Variable.get("PASSWORD_FTP")
POSTGRES_HOOK: PostgresHook = PostgresHook(postgres_conn_id="stat-postgres")

dag = DAG(
    dag_id="sftp-to-database",
    schedule_interval="@daily",
    start_date=datetime(2024, 2, 13, 1, 0, 0),
    catchup=False,
)


def download_zip_from_sftp(
    hostname_ftp: str,
    port_ftp: int,
    username_ftp: str,
    password_ftp: str,
    remote_directory: str,
    local_directory: Path,
):
    transport = paramiko.Transport((hostname_ftp, port_ftp))
    transport.connect(username=username_ftp, password=password_ftp)
    sftp: SFTPClient | None = paramiko.SFTPClient.from_transport(transport)

    if sftp is None:
        raise AirflowNotFoundException("Connect with sftp not found")

    file_times: Dict[str, int | None] = {}
    files_with_times: List[paramiko.SFTPAttributes] = sftp.listdir_attr(
        remote_directory
    )

    for file_attr in files_with_times:
        file_times[file_attr.filename] = file_attr.st_mtime

    latest_file: str = max(file_times, key=file_times.get)

    local_latest_file_path = os.path.join(local_directory, latest_file)

    sftp.get(os.path.join(remote_directory, latest_file), local_latest_file_path)

    with zipfile.ZipFile(local_latest_file_path, "r") as zip_ref:
        zip_files = zip_ref.namelist()

        if len(zip_files) != 1 or not zip_files[0].endswith(".csv"):
            raise AirflowNotFoundException(
                "Архив содержит недопустимое количество или тип файлов"
            )

        csv_file: str = zip_files[0]
        csv_file_path: str = os.path.join(LOCAL_DIRECTORY, csv_file)
        zip_ref.extract(csv_file, LOCAL_DIRECTORY)

    os.remove(local_latest_file_path)

    return csv_file_path


def write_to_database(database: PostgresHook, **kwargs):
    ti = kwargs["ti"]
    file_path = ti.xcom_pull(task_ids="download_zip_from_sftp_task")

    rows: List[Tuple] = []

    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader)

        for row in reader:
            org_code, mnc, tin, org_name, _ = row
            rows.append((org_code, mnc, tin, org_name))

    conn = database.get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM operators")

            sql = "INSERT INTO operators (org_code, mnc, tin, org_name) VALUES (%s, %s, %s, %s)"
            execute_batch(cur, sql, rows, page_size=1000)

        conn.commit()

    except Exception:
        conn.rollback()
        raise AirflowException("Ошибка транзакции")

    finally:
        conn.close()


download_zip_from_sftp_task = PythonOperator(
    task_id="download_zip_from_sftp_task",
    python_callable=download_zip_from_sftp,
    provide_context=True,
    op_kwargs={
        "hostname_ftp": HOSTNAME_FTP,
        "port_ftp": PORT_FTP,
        "username_ftp": USERNAME_FTP,
        "password_ftp": PASSWORD_FTP,
        "remote_directory": REMOTE_DIRECTORY,
        "local_directory": LOCAL_DIRECTORY,
    },
    dag=dag,
)

write_to_database_task = PythonOperator(
    task_id="write_to_database_task",
    python_callable=write_to_database,
    provide_context=True,
    op_kwargs={
        "database": POSTGRES_HOOK,
    },
    dag=dag,
)

download_zip_from_sftp_task >> write_to_database_task
