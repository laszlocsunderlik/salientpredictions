import os
import datetime as dt
from io import StringIO

import requests
from requests.adapters import HTTPAdapter, Retry
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
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
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


def _get_station_name(station: str) -> str:
    return station.split(".")[0]


def _print_infos(df: pd.DataFrame, station_code: str) -> None:
    print("#" * 50)
    print(f"Station: {station_code}")
    print(f"DataFrame Information {station_code}:")
    print(df.info())
    print(f"\nFirst 3 Rows {station_code}:")
    print(df.head(3))
    print(f"\nSummary Statistics {station_code}:")
    print(df.describe())
    print(f"\nMissing Values {station_code}:")
    print(df.isnull().sum())
    print(f"\nCustom Print Statement {station_code}:")
    print(f"The DataFrame has {df.shape[0]} rows and {df.shape[1]} columns.")


def build_ghcnd_archive(file_name: str, response: requests.Response, station_code: str) -> None:
    """
    Builds the initial data archive
    :param file_name: path to export the csv file with the filename
    :param response: requests get response after sending the base URL plus filename to the server
    :param station_code: the station code for every location, example: (USW00014739)
    """
    new_data = pd.read_csv(StringIO(response.text), low_memory=False)
    new_data_mod = _modify_schema(new_data)

    _print_infos(df=new_data_mod, station_code=station_code
    new_data_mod.to_csv(file_name, index=False)

    # Save the execution date and latest datapoint date as Airflow variables
    set_airflow_variables(df=new_data_mod, station_code=station_code)


def update_ghcnd_archive(file_name: str, response: requests.Response, station_code: str) -> None:
    """
    Updates the existing data archive with new data
    :param file_name: path to export the csv file with the filename
    :param response: requests get response after sending the base URL plus filename to the server
    :param station_code: the station code for every location, example: (USW00014739)
    """
    existing_data = pd.read_csv(file_name)
    new_data = pd.read_csv(StringIO(response.text), low_memory=False)

    # Modify the schema of the new data to match the existing data
    new_data_mod = _modify_schema(new_data)

    # Save the execution date and latest datapoint date as Airflow variables
    set_airflow_variables(df=new_data_mod, station_code=station_code)

    # Find rows in new data that are not present in the existing data
    ## TODO!! check this option dropna
    new_rows = new_data_mod[~new_data_mod.isin(existing_data)].dropna()

    if not new_rows.empty:
        updated_data = existing_data.append(new_rows, ignore_index=True)
    else:
        updated_data = existing_data

    _print_infos(df=updated_data, station_code=station_code)
    updated_data.to_csv(file_name, index=False)


def set_airflow_variables(df: pd.DataFrame, station_code: str) -> None:
    latest_datapoint_date = pd.to_datetime(df["DATE"]).max().strftime("%Y-%m-%d")

    # Save the execution date and latest datapoint date as Airflow variables
    Variable.set(f"{station_code}_last_run_date", dt.datetime.now().strftime("%Y-%m-%d"))
    print(f"Last run date for {station_code}: {Variable.get('last_run_date')}")
    Variable.set(f"{station_code}_latest_datapoint_date", latest_datapoint_date)
    print(f"Latest datapoint date for {station_code}: {Variable.get('latest_datapoint_date')}")


with DAG(
    dag_id="NOAA",
    start_date=dt.datetime(year=2024, month=1, day=30),
    end_date=dt.datetime(year=2024, month=2, day=28),
    schedule_interval="@daily",
    catchup=True,
) as dag:
    def fetch_daily_weather_data(station) -> None:
        """Downloads the daily csv file for a given stations"""

        output_path = "../data"
        os.makedirs(output_path, exist_ok=True)

        session, url = _get_session()

        station_code = _get_station_name(station)
        url_station = url + station
        response = requests.get(url_station)
        response.raise_for_status()

        # Include the execution date in the filename
        file_name = f"{output_path}/{station}"

        # Check if the file already exists locally
        if os.path.exists(file_name):
            update_ghcnd_archive(
                file_name=file_name,
                response=response,
                station_code=station_code
            )

        else:
            build_ghcnd_archive(
                file_name=file_name,
                response=response,
                station_code=station_code
            )


    start_task = PythonOperator(
        task_id="start_dag",
        python_callable=lambda: None
    )
    end_task = PythonOperator(
        task_id="end_dag",
        python_callable=lambda: None
    )

    fetch_weather_tasks = []
    for station in STATIONS:
        station_code = _get_station_name(station)
        task_id = f"fetching weather for {station_code}"
        fetch_weather = PythonOperator(
            task_id=task_id,
            python_callable=fetch_daily_weather_data,
            provide_context=True,
            dag=dag,
        )
        fetch_weather_tasks.append(fetch_weather)

    # Set up task dependencies
    start_task >> fetch_weather_tasks >> end_task
