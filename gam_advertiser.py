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




_locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8'])

# Initialize a client object, by default uses the credentials in ~/googleads.yaml.

client = ad_manager.AdManagerClient.LoadFromStorage(r'/Users/christian/Documents/Work/gam_api/ad_manager_keyfile.yml')
report_downloader = client.GetDataDownloader(version='v202005')
end_date = datetime.now().date()
start_date = end_date - Timedelta(days=1)

# Initialize a service.
network_service = client.GetService('NetworkService', version='v202011')

# Make a request.
current_network = network_service.getCurrentNetwork()

print ('Found network %s (%s)!' % (current_network['displayName'],current_network['networkCode']))

# Create report job.

report_job = {
      'reportQuery': {
          'dimensions': ['AD_UNIT_ID', 'AD_UNIT_NAME', 'ADVERTISER', 'PROGRAMMATIC_BUYER'],
          'columns': ['TOTAL_CODE_SERVED_COUNT','TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 
          'TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS',
          'TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE',
          'TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM', 
          'TOTAL_LINE_ITEM_LEVEL_CLICKS',
          'TOTAL_LINE_ITEM_LEVEL_CTR'],
          'dateRangeType': 'CUSTOM_DATE',
          'startDate': start_date,
          'endDate': end_date
      }
  }



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
df['Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'] = df['Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'].div(1000000).round(2)
df['Column.TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM'] = df['Column.TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM'].div(1000000).round(2)
df['Column.TOTAL_LINE_ITEM_LEVEL_CTR'] = df['Column.TOTAL_LINE_ITEM_LEVEL_CTR'].round(3)
df.rename(columns={
  'Dimension.AD_UNIT_ID' : 'ad_unit_id',
  'Dimension.AD_UNIT_NAME' : 'ad_unit_name',
  'Column.TOTAL_CODE_SERVED_COUNT' : 'total_code_served',
  'Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS' : 'total_impressions',
  'TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS' : 'total_viewable_impressions',
  'TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE' : 'total_revenue',
  'TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM' : 'total_avg_ecpm',
  'TOTAL_LINE_ITEM_LEVEL_CLICKS' : 'total_clicks',
  'Column.TOTAL_LINE_ITEM_LEVEL_CTR' : 'total_ctr'
}, inplace=True)



try: 
  df.to_csv(r'/Users/christian/Documents/Work/gam_api/output_data/gam_advertiser.csv', index=False)
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

dff = pd.read_csv(r'/Users/christian/Documents/Work/gam_api/output_data/gam_advertiser.csv')

#INSERT COLUMN NAMING CONVENTION

def copy_from_dataframe(conn, dff):
    
    try:
        dff.to_sql('gam_advertiser',conn, if_exists='replace')
        print("Data inserted using copy_from_dataframe successfully....")
    except (Exception, psycopg2.DatabaseError) as error:
        show_psycopg2_exception(error)
        
        
# Connect to the database
conn = connect(engine)
conn.autocommit = True
# Run the mogrify() method
copy_from_dataframe(conn, dff)


default_args = {
  'owner' : 'christian',
  'depends_on_past' : False, 
  'email' : 'christian.faulkner@network-n.com',
  'email_on_failure' : True,
  'email_on_retry' : False,
  'execution_timeout': timedelta(seconds=1000)
}

dag = DAG(
   'gam_advertiser',
    default_args=default_args,
    description='Daily google API ETL and Postgres import',
    schedule_interval= '@hourly',
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