from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import pandas as pd
##################################################################################################
FILE_PATH = '/home/akhilesh/big_data_lab'

default_args = {
    'owner': 'akhilesh',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define a DAG
dag = DAG(
    'noaa_data_analytics_pipeline',
    default_args=default_args,
    description='Pipeline to process NOAA data in parallel using Apache Beam',
    schedule_interval=None,
)
##################################################################################################
# Task 1: Sensing the file archive
wait_for_archive_task = FileSensor(
    task_id='wait_for_archive',
    fs_conn_id='conn_file_sensor',
    filepath='noaa_archive.zip',
    poke_interval=1,  # seconds
    timeout=5,  # seconds
    mode='poke',
    dag=dag,
)
##################################################################################################
# Task 2: Check if the file is a valid archive and unzip it
check_and_unzip_task = BashOperator(
    task_id='check_and_unzip',
    bash_command=f'if [[ $(file -b --mime-type "noaa_archive.zip") == "application/zip" ]]; then unzip -o noaa_archive.zip; fi',
    dag=dag,
)
##################################################################################################

COLS = ['LATITUDE','LONGITUDE','HourlyDewPointTemperature','HourlyDryBulbTemperature',
        'HourlyRelativeHumidity','HourlyWindDirection','HourlyWindGustSpeed','HourlyWindSpeed']

from apache_beam import DoFn, ParDo, Pipeline
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import Map
import os

def process_and_filter_csv(files):    
    
    output_path = '/home/akhilesh/big_data_lab/'    
    class ProcessAndFilterFn(DoFn):
        def process(self, element):
            # Read the CSV file into a Pandas DataFrame
            df = pd.read_csv(element)
            
            # Filter the DataFrame based on required fields            
            filtered_df = df[COLS]

            # Convert the DataFrame to a list of dictionaries
            data_list = filtered_df.to_dict(orient='records')
            
            # Extract Lat/Long values and create a tuple
            lat_long_tuple = (filtered_df['LATITUDE'].iloc[0], filtered_df['LONGITUDE'].iloc[0], data_list)
            
            yield lat_long_tuple
    
    options = PipelineOptions(
    runner='DirectRunner',
    project='noaa-data-pipeline',
    job_name='process-and-filter-csv',
    temp_location='/tmp',
    direct_num_workers=2,
    direct_running_mode='multi_processing'
    )
    with Pipeline(options=options) as pipeline:
        # Read the CSV files
        csv_data = pipeline | 'Read CSV Files' >> ReadFromText(files, skip_header_lines=1)
        
        # Process and filter the CSV data
        processed_data = csv_data | 'Process and Filter CSV' >> ParDo(ProcessAndFilterFn())
        
        # Write the output to a text file
        _ = processed_data | 'Write Output' >> WriteToText(os.path.join(output_path, 'output.txt'))

# Task 3: Process CSV files and extract required fields
process_csv_task = PythonOperator(
    task_id='process_and_filter_csv',
    python_callable=process_and_filter_csv,
    provide_context=True,
    op_args=[f'{FILE_PATH}/{filename}' for filename in os.listdir('/home/akhilesh/big_data_lab') if filename.endswith('.csv')],    
    dag=dag,
)

##################################################################################################

def compute_monthly_averages(input_path, output_path):

    class ComputeMonthlyAveragesFn(DoFn):
        def process(self, element):
            lat, lon, data_list = element

            # Convert the list of dictionaries to a Pandas DataFrame
            df = pd.DataFrame(data_list)

            # Extract month from the timestamp
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            df['Month'] = df['Timestamp'].dt.month

            # Compute monthly averages for the required fields
            monthly_averages = df.groupby('Month').mean()

            # Convert the DataFrame to a list of dictionaries
            averages_list = monthly_averages.to_dict(orient='records')

            # Combine lat, lon, and monthly averages into the final output
            result = (lat, lon, averages_list)

            yield result

    options = PipelineOptions(
    runner='DirectRunner',
    project='noaa-data-pipeline',
    job_name='compute-monthly-averages',
    temp_location='/tmp',
    direct_num_workers=2,
    direct_running_mode='multi_processing' #multi_threading | multi_processing | in_memory    
    )
    with Pipeline(options=options) as pipeline:
        # Read the processed data from the previous task
        processed_data = pipeline | 'Read Processed Data' >> ReadFromText(input_path)

        # Compute monthly averages
        monthly_averages = processed_data | 'Compute Monthly Averages' >> ParDo(ComputeMonthlyAveragesFn())

        # Write the output to a text file
        _ = monthly_averages | 'Write Monthly Averages' >> WriteToText(output_path)

# Task 4: Compute monthly averages using Apache Beam
compute_averages_task = PythonOperator(
    task_id='compute_monthly_averages',
    python_callable=compute_monthly_averages,
    provide_context=True,
    op_args=[os.path.join('/home/akhilesh/big_data_lab/', 'output.txt'),
             os.path.join('/home/akhilesh/big_data_lab/', 'monthly_averages.txt')],
    dag=dag,
)
##################################################################################################
import geopandas as gpd
import geodatasets as gds
import matplotlib.pyplot as plt
import os

def create_geospatial_visualizations(input_path, output_path):    

    class CreateVisualizationsFn(DoFn):
        def process(self, element):
            lat, lon, averages_list = element

            # Convert averages_list to a GeoDataFrame
            gdf = gpd.GeoDataFrame({'geometry': gpd.points_from_xy([lon], [lat])})

            # Plot heatmaps for each required field
            for field in averages_list[0].keys():
                field_values = [avg[field] for avg in averages_list]
                gdf[field] = field_values

                # Create a heatmap using geodatasets
                fig, ax = plt.subplots(1, 1, figsize=(10, 10))
                gds.plot_heatmap(gdf, field, ax=ax, alpha=0.7, cmap='viridis')

                # Save the plot as a PNG file
                plt.savefig(os.path.join(output_path, f'{field}_heatmap.png'))
                plt.close()

                yield f'Heatmap plot for {field} saved successfully.'

    options = PipelineOptions(
    runner='DirectRunner',
    project='noaa-data-pipeline',
    job_name='create-visualization',
    temp_location='/tmp',
    direct_num_workers=2,
    direct_running_mode='multi_processing' #multi_threading | multi_processing | in_memory    
    )
    with Pipeline(options=options) as pipeline:
        # Read the monthly averages data from the previous task
        monthly_averages = pipeline | 'Read Monthly Averages' >> ReadFromText(input_path)

        # Create geospatial visualizations
        _ = monthly_averages | 'Create Visualizations' >> ParDo(CreateVisualizationsFn())

# Task 5: Create geospatial visualizations using Apache Beam
create_visualizations_task = PythonOperator(
    task_id='create_geospatial_visualizations',
    python_callable=create_geospatial_visualizations,
    provide_context=True,
    op_args=[os.path.join('/home/akhilesh/big_data_lab/', 'monthly_averages.txt'),
             os.path.join('/home/akhilesh/big_data_lab/', 'visualizations')],
    dag=dag,
)

##################################################################################################
# Task 6: Delete the CSV files from the destined folder
delete_csv_task = BashOperator(
    task_id='delete_csv_files',
    bash_command=f'rm -f {FILE_PATH}/*.csv',
    trigger_rule='all_success',  # Or 'all_failed', 'one_failed', etc.
    dag=dag,
)

##################################################################################################

wait_for_archive_task >> check_and_unzip_task >> process_csv_task >> compute_averages_task >> create_visualizations_task >>  delete_csv_task
