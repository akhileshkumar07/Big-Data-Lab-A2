# NOAA Data Fetch Pipeline

This pipeline is designed to fetch and process NOAA (National Centers for Environmental Information) data using Apache Airflow. It automates the process of fetching data from a specified URL, extracting relevant CSV file links, selecting random files from the available list, fetching those files, zipping them, and moving the archive to a desired location.

## Overview

The pipeline consists of several tasks:

1. **Fetch Page Task (`fetch_page`)**: This task fetches the webpage of the datasets from the specified URL and saves it as `page_fetch.html`.

2. **Extract Links Task (`extract_links`)**: This task parses the HTML content of the fetched page (`page_fetch.html`) and extracts links to CSV files.

3. **Select Files Task (`select_files`)**: Randomly selects a predefined number of CSV files from the list of extracted links.

4. **Fetch Files Task (`fetch_files`)**: Downloads the selected CSV files to a specified output path.

5. **Zip Files Task (`zip_files`)**: Zips the downloaded CSV files into a single archive named `noaa_archive.zip`.

6. **Place Archive Task (`place_archive`)**: Moves the generated archive (`noaa_archive.zip`) to a desired location.

## Parameters

- **Base URL**: The base URL from which the datasets are fetched.
- **Year**: The specific year for which data is being fetched.
- **Output Path**: The directory where the fetched data and archive are saved.

## Usage

1. Ensure you have Apache Airflow installed and configured.
2. Copy the provided code into your Airflow DAGs directory.
3. Adjust the parameters such as `BASE_URL`, `YEAR`, and `OUTPUT_PATH` as per your requirements.
4. The pipeline is set to start from the specified `start_date` (default is `2024-01-01`).
5. Execute the Airflow scheduler to start the DAG execution.

## Dependencies

- **BeautifulSoup**: Used for HTML parsing to extract CSV links.
- **wget**: Command-line utility used for fetching webpages and files.
- **subprocess**: Python module used for executing shell commands.
- **random**: Python module used for random selection of files.



# NOAA Data Analytics Pipeline

This Airflow DAG (Directed Acyclic Graph) represents a data analytics pipeline designed to process NOAA (National Oceanic and Atmospheric Administration) data in parallel using Apache Beam. The pipeline consists of several tasks to sense, process, analyze, and visualize the NOAA data. Below is the description of each task in the pipeline:

### Tasks:

1. **wait_for_archive_task**: File Sensor Task
   - Description: Monitors the presence of a file named `noaa_archive.zip` in the specified directory.
   - Dependencies: None
   - Trigger Rule: None

2. **check_and_unzip_task**: Bash Operator Task
   - Description: Checks if the `noaa_archive.zip` file is a valid zip archive and unzips it if it is.
   - Dependencies: `wait_for_archive_task`
   - Trigger Rule: All Success

3. **process_csv_task**: Python Operator Task
   - Description: Processes CSV files, extracts required fields, and filters the data using Apache Beam.
   - Dependencies: `check_and_unzip_task`
   - Trigger Rule: All Success

4. **compute_averages_task**: Python Operator Task
   - Description: Computes monthly averages of the processed data using Apache Beam.
   - Dependencies: `process_csv_task`
   - Trigger Rule: All Success

5. **create_visualizations_task**: Python Operator Task
   - Description: Creates geospatial visualizations (heatmaps) based on the computed monthly averages.
   - Dependencies: `compute_averages_task`
   - Trigger Rule: All Success

6. **delete_csv_task**: Bash Operator Task
   - Description: Deletes the CSV files from the destination folder after processing.
   - Dependencies: `create_visualizations_task`
   - Trigger Rule: All Success


### How to Run:
1. Ensure that Airflow is properly configured and running.
2. Place the `noaa_archive.zip` file in the specified directory.
3. Trigger the DAG manually or through external events.
4. Monitor the progress of tasks in the Airflow UI.
5. Once all tasks are successfully completed, the desired analytics results and visualizations will be available in the designated output directory.

### Note:
- This pipeline assumes the presence of CSV files in the specified directory (`/home/akhilesh/big_data_lab`) for processing. Ensure that the directory contains the required input files.
- Adjustments to the file paths, dependencies, or task configurations may be necessary based on specific requirements and environment setup.