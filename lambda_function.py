import json
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import warnings
from pandas.core.common import SettingWithCopyWarning
import us_covid_etl

NYT_FILE_PATH = os.environ['NYT_FILE_PATH']
JH_FILE_PATH = os.environ['JH_FILE_PATH']
SNS_TOPIC_ARN =  os.environ['SNS_TOPIC_ARN']
DYNAMODB_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']

sns = boto3.client('sns')
dynamodb = boto3.resource("dynamodb")

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

def lambda_handler(event, context):
    try:
        covid_df = us_covid_etl.prepare_data(NYT_FILE_PATH, JH_FILE_PATH)
    except Exception as e:
        print('Error while preparing data:' + str(e))
        send_notification('ETL job failed during processing of csv file. Error: '+str(e), 'ETL job result')
        exit(1)
    print('Retrieved data successfully.')
    try:
        save_data(covid_df)
    except ClientError as e:
        print(f'Error while saving data: {e.response["Error"]["Message"]}')
    except Exception as e:
        print('Error while saving data:' + str(e))
        send_notification('ETL job failed during saving the data. Error: '+str(e), 'ETL job result')
        exit(1)
    
def save_data(df):
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    latest_date = get_latest_updated_date(table)

    # determine the subset of data to insert
    if latest_date is not None:
        df_new = df[df['date'].dt.date>latest_date]
    else:
        df_new = df

    print('Inserting data into DynamoDb table...')
    inserted_rows = insert_new_data(table, df_new)
    
    print('Data inserted successfully. Sending notification...')
    send_notification(f'Covid data ETL succeeded. Processed {inserted_rows} rows.', 'ETL job result')


def get_latest_updated_date(table):
    response = table.scan()
    table_data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"] )
        table_data.extend(response["Items"])
    
    latest_upd_date = None
    if len(table_data)!=0:
        table_data_as_df = pd.DataFrame(table_data)
        table_data_as_df['date'] = pd.to_datetime(table_data_as_df['date'])
        latest_upd_date = table_data_as_df['date'].max()
    
    return latest_upd_date

def insert_new_data(table, df):
    inserted_rows = 0
    for row in df.itertuples():
        table.put_item(
            Item = {
                'date': row.date.strftime(us_covid_etl.CSV_DATE_FORMAT),
                'cases': row.cases,
                'deaths': row.deaths,
                'recovered': row.recovered
                },
            ConditionExpression = 'attribute_not_exists(#dt)',
            ExpressionAttributeNames = {'#dt': 'date'}
            )
        inserted_rows += 1
    return inserted_rows
        

def send_notification(message, subject):
    response = sns.publish(
        TargetArn = SNS_TOPIC_ARN,
        Message = message,
        Subject = subject
        )