import os
import datetime as dt
from io import StringIO

import requests
import pandas as pd
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

BASE_RUL = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/"
STATIONS = ["USW00014739.csv", "MXN00023169.csv", "USW00094846.csv"]


def _get_session() -> (requests.Session, str):
    """Builds a request session for the NOAA website
    :return: requests session, base_url
    """
    session = requests.Session()
    base_url = BASE_RUL
    return session, base_url


def _modify_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Modifies the schema of the dataframe to match the table schema
    :param df: pandas DataFrame object
    :return df: schema modified DataFrame object
    """
    df = df.rename(columns={"PRCP": "PRECP"})
    my_cols = [
        "STATION",
        "DATE",
        "PRECP",
        "TMAX",
        "TMIN"
    ]
    return df[df.columns.intersection(my_cols)]


def build_ghcnd_archive(file_name: str, response: requests.Response) -> None:
    """
    Builds the initial data archive
    :param file_name: path to export the csv file with the filename
    :param response: requests get response after sending the base URL plus filename to the server
    """
    new_data = pd.read_csv(StringIO(response.text), low_memory=False)
    new_data_mod = _modify_schema(new_data)
    new_data_mod.to_csv(file_name, index=False)

    # Save the execution date and latest datapoint date as Airflow variables
    Variable.set("last_run_date", dt.datetime.now().strftime("%Y-%m-%d"))
    print(f"Last run date: {Variable.get('last_run_date')}")


def update_ghcnd_archive(file_name: str, response: requests.Response) -> None:
    """
    Updates the existing data archive with new data
    :param file_name: path to export the csv file with the filename
    :param response: requests get response after sending the base URL plus filename to the server
    """
    existing_data = pd.read_csv(file_name)
    new_data = pd.read_csv(StringIO(response.text), low_memory=False)

    # Modify the schema of the new data to match the existing data
    new_data_mod = _modify_schema(new_data)

    # Find rows in new data that are not present in the existing data
    # TODO check this option dropna
    new_rows = new_data_mod[~new_data_mod.isin(existing_data)].dropna()

    if not new_rows.empty:
        updated_data = existing_data.append(new_rows, ignore_index=True)
        latest_datapoint_date = pd.to_datetime(new_data_mod["DATE"]).max().strftime("%Y-%m-%d")
        Variable.set("latest_datapoint_date", latest_datapoint_date)
        print(f"Latest datapoint date: {Variable.get('latest_datapoint_date')}")
    else:
        updated_data = existing_data
    updated_data.to_csv(file_name, index=False)


def get_daily_csv() -> None:
    """Downloads the daily csv file for a given stations"""

    output_path = "../data"
    os.makedirs(output_path, exist_ok=True)

    session, url = _get_session()

    for file in STATIONS:
        url_station = url + file
        response = requests.get(url_station)
        response.raise_for_status()

        # Include the execution date in the filename
        file_name = f"{output_path}/{file}"

        # Check if the file already exists locally
        if os.path.exists(file_name):
            update_ghcnd_archive(file_name=file_name, response=response)

        else:
            build_ghcnd_archive(file_name=file_name, response=response)


dag = DAG(
    dag_id="NOAA",
    start_date=dt.datetime(year=2024, month=1, day=30),
    end_date=dt.datetime(year=2024, month=2, day=28),
    schedule_interval="@daily",
    catchup=False,
)

fetch_weather = PythonOperator(
    task_id="fetch_weather",
    python_callable=get_daily_csv,
    provide_context=True,
    # op_kwargs={
    #     "execution_date": "{{ ds }}",
    # },
    # templates_dict={"output_path": "/data/{{ ds }}"},
    dag=dag,
)
