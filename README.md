# **Salient Predictions Task**

## **Description of the GitHub repository:**

This script is an Apache Airflow DAG (Directed Acyclic Graph) designed to download daily weather data from NOAA (National Oceanic and Atmospheric Administration) for specific weather stations: 14739 (Boston), 23169 (Las Vegas), 94846 (Chicago). The script organizes the data into a local archive and updates the archive with new data when the DAG is run. The exported file format is *.csv. 

Airflow was configured based on the official docker compose file (`docker-compose.yaml`) which will allow you to quickly get Airflow up and running with CeleryExecutor in Docker. The provided `Dockerfile` is for install addition requirements for the project like pandas and requests library.

### **Data Source:**

The weather data is sourced from the NOAA GHCND (Global Historical Climatology Network-Daily) dataset.

### **Data Archiving:**

Two functions, `build_ghcnd_archive` and `update_ghcnd_archive`, handle the creation and update of data archives. The schema of the data is modified to match the desired format, and a set of information about the DataFrame is printed.

### **Stations and Base URL:**

The script defines a list of weather station filenames (`STATIONS`) and a base URL (`BASE_URL`) for downloading data.
Example for Boston: https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/USW000014739.csv

### **DAG Configuration:**

The DAG is configured with a start date, end date, and a daily schedule interval. It is set to catch up on missed runs (`catchup=True`).

### **Python Functions:**


`_get_session()`: 

Creates a requests session for accessing NOAA website data. Configure a retry mechanism for HTTP requests made through the session object. The specified retry logic will be applied for a maximum of 5 retries, with an increasing delay between retries, and specifically for the HTTP status codes 500, 502, 503, and 504

`_modify_schema(df)`: 

Modifies the schema of a DataFrame to match the desired table schema.

`_get_station_name(station)`: 

Returns from the STATIONS list the station code. For example from `USW00014739.csv` ——> USW00014739.csv

`_print_infos(df)`: 

print statements of the archive content

`build_ghcnd_archive(file_name, response, station_code)`: 

Builds the initial data archive and sets Airflow variables for latest DAG run vs. latest date in the DataFrame for every station using station code, example: (USW00014739).

`update_ghcnd_archive(file_name, response, station_code)`: 

Updates an existing data archive with new data.Sets Airflow variables for latest DAG run vs. latest date in the DataFrame for every station using station code, example: (USW00014739).

`set_airflow_variable`: 

Sets the latest DAG run and the latest date in the DataFrame as an Airflow Variable type for each station. Used for later analysis to track the different between latest run and latest datapoint.

`fetch_daily_weather_data()`: 

Downloads daily weather data, checks if it exists locally, and either builds the archive or updates it.

### **Airflow Variables:**

Airflow variables (`last_run_date` and `latest_datapoint_date`) are used to store information about the last DAG run date and the latest datapoint date.

### **DAG Execution:**

First task is a dummy task called `start_dag`. 

The next downstream tasks are (`f"fetching weather for {station_code}"`) is implemented as a PythonOperator, which calls the `fetch_daily_weather_data()` function as many times we have a station in the STATIONS list. 

Last task is a dummy `end_task` task.


### **Execution Flow:**

For each weather station in the STATIONS list, the script checks if the data file already exists locally. If it exists, it updates the existing archive; otherwise, it builds the initial archive.

### **Output Path:**

The data is saved in the "../data" directory, and this directory is created if it doesn't exist.

### **Execution Dates and Airflow Variables:**

Airflow variables are used to store and retrieve information about the DAG execution, providing a way to track the last run date and the latest datapoint date.

### **Purpose:**

The script is designed to automate the download and archiving of daily weather data from NOAA for specific stations. The DAG structure allows for regular updates to the data archive, ensuring that the dataset is kept current. The use of Airflow variables enhances tracking and monitoring of DAG execution details and be able to compare the latest run date and the latest timepoints in the datasets for each station. Useful for later analysis and correct comparisons between stations.


### **How to run the script:**

1. Clone the repository to your local machine.
2. Install Docker and Docker Compose.
3. `mkdir -p ./dags ./logs ./plugins ./config ./data`
4. `echo -e "AIRFLOW_UID=$(id -u)" > .env`
3. Run `docker-compose build` to create the Docker image.
3. Run `docker-compose up airflow-init` to create the necessary tables in the Airflow database.
4. Run `docker-compose up` to start the Airflow web server, scheduler, and worker.
5. Navigate to `http://localhost:8080` in your browser and enable the `ghcnd_weather_data_dag` DAG.
6. Trigger the DAG.
