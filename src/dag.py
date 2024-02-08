import os

from datetime import datetime

import csv
import paramiko
import zipfile
import psycopg2 as pg
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from pprint import pprint


# Класс с конфигурацией
class Settings:
    pass


# TODO create default args

engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)

HOSTNAME = "prod-sftp.bdpn.ru"
USERNAME = "mnctel01"
PASSWORD = "lD64w6uB"
PORT = 3232
REMOTE_DIRECTORY = "/numlex/Operators/"
LOCAL_DIRECTORY = "/home/roman/projects/stat-567/"


postgres_hook = PostgresHook(postgres_conn_id="tmp_connection")

dag = DAG(
    dag_id="tmp",
    schedule_interval=None,
    start_date=datetime(2024, 2, 7),
    catchup=False,
)


def download_zip_from_sftp():
    transport = paramiko.Transport((HOSTNAME, PORT))
    transport.connect(username=USERNAME, password=PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)

    file_times = {}
    files_with_times = sftp.listdir_attr(REMOTE_DIRECTORY)

    for file_attr in files_with_times:
        file_times[file_attr.filename] = file_attr.st_mtime

    latest_file = max(file_times, key=file_times.get)

    local_latest_file_path = os.path.join(LOCAL_DIRECTORY, latest_file)

    sftp.get(os.path.join(REMOTE_DIRECTORY, latest_file), local_latest_file_path)
    with zipfile.ZipFile(local_latest_file_path, "r") as zip_ref:
        zip_ref.extractall(LOCAL_DIRECTORY)

    return local_latest_file_path


def write_to_database(**kwargs):
    # ti = kwargs["ti"]
    # file_path = ti.xcom_pull(task_id="download_zip_from_sftp_task")
    file_path = "/home/roman/projects/stat-567/Operators_202402060000_3731.csv"

    rows = []

    with open(file_path, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader)

        for row in reader:
            org_code, mnc, tin, org_name, _ = row
            rows.append((org_code, mnc, tin, org_name))
    return rows

pprint(write_to_database())