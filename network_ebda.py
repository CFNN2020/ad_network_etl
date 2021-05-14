from googleads import ad_manager
from googleads import ad_manager
from googleads import errors
import io
import tempfile
import locale
import pandas as pd
from pandas import datetime
from pandas import Timedelta
import csv
import re
import gzip
import _locale
import schedule
import sqlalchemy as sqla
import sys
import os
import datetime as dt
from datetime import timedelta
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import psycopg2
from psycopg2 import OperationalError



saved_query_id = 12132808842
_locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8'])

# Initialize a client object, by default uses the credentials in ~/googleads.yaml.

client = ad_manager.AdManagerClient.LoadFromStorage(r'/Users/christian/Documents/Work/gam_api/ad_manager_keyfile.yml')
report_downloader = client.GetDataDownloader(version='v202005')
end_date = datetime.now().date()
start_date = end_date - Timedelta(days=7)

# Initialize a service.
network_service = client.GetService('NetworkService', version='v202011')

# Make a request.
current_network = network_service.getCurrentNetwork()

print ('Found network %s (%s)!' % (current_network['displayName'],current_network['networkCode']))

# Create report job.
def main(client, saved_query_id):
  # Initialize appropriate service.
  report_service = client.GetService('ReportService', version='v202105')

  # Initialize a DataDownloader.
  report_downloader = client.GetDataDownloader(version='v202105')

  # Create statement object to filter for an order.
  statement = (ad_manager.StatementBuilder(version='v202105')
               .Where('id = :id')
               .WithBindVariable('id', int(saved_query_id))
               .Limit(1))

  response = report_service.getSavedQueriesByStatement(
      statement.ToStatement())

  if 'results' in response and len(response['results']):
    saved_query = response['results'][0]

  if saved_query['isCompatibleWithApiVersion']:
    report_job = {}

    # Set report query and optionally modify it.
    report_job['reportQuery'] = saved_query['reportQuery']


# Initialize a DataDownloader.
report_downloader = client.GetDataDownloader(version='v202011')
try:
    # Run the report and wait for it to finish.
    report_job_id = report_downloader.WaitForReport(report_job)
except errors.AdManagerReportError as e:
    print('Failed to generate report. Error was: %s' % e)

  # Change to export format.
export_format = 'CSV_DUMP'

report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False,)

  # Download report data.
report_downloader.DownloadReportToFile(
    report_job_id, 'CSV_DUMP', report_file)


  # Use pandas to join the two csv files into a match table
report = pd.read_csv(report_file.name)
df = pd.DataFrame(report)
pd.set_option('display.max_columns', None)
#df['Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'] = df['Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE	'].div(1000000).round(2)
df.rename(columns={
  'Dimension.YIELD_GROUP_NAME' : 'Yield_partner',
  #'Dimension.VIDEO_PLACEMENT_NAME' : 'Video_placement',
  #'Dimension.PROGRAMMATIC_CHANNEL_NAME' : 'Programmatic_channel',
  'Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS' : 'Impressions',
  'Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE' : 'Total_CPM_and_CPC_revenue'
}, inplace=True)



try: 
  df.to_csv(r'/Users/christian/Documents/Work/gam_api/output_data/network_ebda.csv', index=False)
  print('Report downloaded successfully')
except errors.AdManagerReportError as e: 
  print('Failed to download report. Error was: %s' % e)

engine = sqla.create_engine('postgresql://doadmin:patxi1ctdovgrml4@etl-db-do-user-9163711-0.b.db.ondigitalocean.com:25060/gam_etl', pool_pre_ping=True)

def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()    
    # get the line number when exception occured
    line_n = traceback.tb_lineno    
    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type) 
    # psycopg2 extensions.Diagnostics object attribute

def connect(engine):
    conn = None
    try:
        print('Connecting to the PostgreSQL[programmatic_reporting]....')
        conn = engine
        print("Connection successful....")
        
    except OperationalError as err:
        # passing exception to function
        show_psycopg2_exception(err)        
        # set the connection to 'None' in case of error
        conn = None
    return conn

dff = pd.read_csv(r'/Users/christian/Documents/Work/gam_api/output_data/network_ebda.csv')

#INSERT COLUMN NAMING CONVENTION

def copy_from_dataframe(conn, dff):
    
    try:
        dff.to_sql('network_ebda',conn, if_exists='append')
        print("Data inserted using copy_from_dataframe successfully....")
    except (Exception, psycopg2.DatabaseError) as error:
        show_psycopg2_exception(error)
        
        
# Connect to the database
conn = connect(engine)
conn.autocommit = True
# Run the mogrify() method
copy_from_dataframe(conn, dff)

if __name__ == '__main__':
  # Initialize client object.
  ad_manager_client = ad_manager.AdManagerClient.LoadFromStorage()
  main(ad_manager_client, SAVED_QUERY_ID)

default_args = {
  'owner' : 'christian',
  'depends_on_past' : False, 
  'email' : 'christian.faulkner@network-n.com',
  'email_on_failure' : True,
  'email_on_retry' : False,
  'execution_timeout': timedelta(seconds=1000)
}

dag = DAG(
   'network_ebda',
    default_args=default_args,
    description='Daily google API ETL and Postgres import',
    schedule_interval= '@daily',
    start_date=datetime(2021, 4, 16, 0, 0,),
    tags=['google_ad_api'],
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = PythonOperator(
    task_id='initiate_database_connection',
    depends_on_past=False,
    python_callable=connect,
    op_kwargs={'engine' : 'conn' },
    retries=3,
    dag=dag,
)

t3 = PythonOperator(
    task_id='data_upload',
    depends_on_past=False,
    python_callable=copy_from_dataframe,
    op_kwargs={'conn': 'connect(engine)',
    'dff' : 'dff'},
    retries=3,
    dag=dag,
)


t1 >> t2 
t2 >> t3 