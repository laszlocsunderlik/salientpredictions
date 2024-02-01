Salient Predictions Task

Repository of the project: 
https://github.com/laszlocsunderlik/salientpredictions

Description of the GitHub repository:

This script is an Apache Airflow DAG (Directed Acyclic Graph) designed to download daily weather data from NOAA (National Oceanic and Atmospheric Administration) for specific weather stations: 14739 (Boston), 23169 (Las Vegas), 94846 (Chicago). The script organizes the data into a local archive and updates the archive with new data when the DAG is run. The exported file format is *.csv. 

Airflow was configured based on the official docker compose file (`docker-compose.yaml`) which will allow you to quickly get Airflow up and running with CeleryExecutor in Docker. The provided `Dockerfile` is for install addition requirements for the project like pandas and requests library.

Data Source:
The weather data is sourced from the NOAA GHCND (Global Historical Climatology 		Network-Daily) dataset.
Data Archiving:
Two functions, `build_ghcnd_archive` and `update_ghcnd_archive`, handle the creation and update of data archives. The schema of the data is modified to match the desired format, and a set of information about the DataFrame is printed.

Stations and Base URL:
The script defines a list of weather station filenames (`STATIONS`) and a base URL (`BASE_URL`) for downloading data.
Example for Boston: https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/USW000014739.csv

DAG Configuration:
The DAG is configured with a start date, end date, and a daily schedule interval. It is set to catch up on missed runs (`catchup=True`).

Python Functions:
`_get_session()`: Creates a requests session for accessing NOAA website data.

`_modify_schema(df)`: Modifies the schema of a DataFrame to match the desired table schema.

`_get_station_name(station)`: Returns from the STATIONS list the station code. For example from `USW00014739.csv` ——> USW00014739.csv

`_print_infos(df): print statements of the archive content

`build_ghcnd_archive(file_name, response, station_code)`: Builds the initial data archive and sets Airflow variables for latest DAG run vs. latest date in the DataFrame for every station using station code, example: (USW00014739).

`update_ghcnd_archive(file_name, response, station_code)`: Updates an existing data archive with new data.Sets Airflow variables for latest DAG run vs. latest date in the DataFrame for every station using station code, example: (USW00014739).

`set_airflow_variable`: Sets the latest DAG run and the latest date in the DataFrame as an Airflow Variable type for each station. Used for later analysis to track the different between latest run and latest datapoint.

`fetch_daily_weather_data()`: Downloads daily weather data, checks if it exists locally, and either builds the archive or updates it.

Airflow Variables:
Airflow variables (`last_run_date` and `latest_datapoint_date`) are used to store information about the last DAG run date and the latest datapoint date.

DAG Execution:
First task is a dummy task called `start_dag`. 

The next downstream tasks are (`f"fetching weather for {station_code}"`) is implemented as a PythonOperator, which calls the `fetch_daily_weather_data()` function as many times we have a station in the STATIONS list. 

Last task is a dummy `end_task` task.


Execution Flow:
For each weather station in the STATIONS list, the script checks if the data file already exists locally. If it exists, it updates the existing archive; otherwise, it builds the initial archive.

Output Path:
The data is saved in the "../data" directory, and this directory is created if it doesn't exist.

Execution Dates and Airflow Variables:
Airflow variables are used to store and retrieve information about the DAG execution, providing a way to track the last run date and the latest datapoint date.

Purpose:
The script is designed to automate the download and archiving of daily weather data from NOAA for specific stations. The DAG structure allows for regular updates to the data archive, ensuring that the dataset is kept current. The use of Airflow variables enhances tracking and monitoring of DAG execution details and be able to compare the latest run date and the latest timepoints in the datasets. Useful for later analysis and correct comparisons between stations.


How would you orchestrate this system to run at scale?
Based on my solution with using Apache Airflow, the system is scalable, because we are able to track each day’s runs. We can rerun failed tasks and we are storing the variables as Airflow Variables about every run. If we want to implement additional downstream tasks for example analysing the datasets PySaprk would be a good option to run analysis at a clustered environment. And because Airflow support PySpark operators, it is worth to mention this possibility.


What major risks would this system face?
The script relies on internet connectivity to access NOAA website, which is very slow, and lot of time irresponsive, leading to issues with data parsing especially in the case when we want to use all locations. Some retry functionality would help if the response status is other the 200-OK. The script writes data as css files. Issues with filesystem permissions or insufficient disk space may prevent successful data storage. Implement proper error handling for filesystem issues. Consider using a distributed file storage system or cloud storage for scalability.

What are the next set of enhancements you would add?
I think based on the point of view of a data scientist or customer for using this datasets it is hard to search and filter for specific locations or timepoints using this css files. So other future improvement would be to store these datasets in a PostgreSQL database with PostGIS extension to be able to store the coordinates as geometry. And building an API on top of this database, with good configurable parameters, like `start_date`,  `end_date` or any region/country based filters and serve the results as a GeoJson. With the mention plus additional parameters the users can easily specify query parameters as URL parameters and retrieve just the data what they need for their purpose. We can easy use also any spatial based filters.

How would you improve the clarity of this assignment?
I found the assignment to be clear and easily understandable. The instructions provided were straightforward, and I didn't encounter any difficulties in interpreting the requirements. Overall, I felt comfortable with the assignment.



