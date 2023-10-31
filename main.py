import boto3
import os
from datetime import datetime
from src.ReportScript import query_table, answered_calls, unanswered_calls, clean_data


TABLE_NAME_1= os.environ["TABLE_NAME_1"]
TABLE_NAME_2= os.environ["TABLE_NAME_2"]
BUCKET_NAME= os.environ["BUCKET_NAME"]
REGION = os.environ['REGION']

# Initialize the clients
db_client = boto3.client("dynamodb", region_name=REGION)
db_resource = boto3.resource("dynamodb", region_name=REGION)
s3_client = boto3.client("s3", region_name=REGION)

def main (event,context):

    #To be set every campaign
    Date_1_start_timestamp = "2023-10-31T09:00:00+08:00"
    Date_1_end_timestamp ="2023-10-31T18:10:00+08:00"
    Date_2_start_timestamp = "2023-11-01T09:00:00+08:00"
    Date_2_end_timestamp = "2023-11-01T18:10:00+08:00"
    Date_3_start_timestamp = "2023-11-02T09:00:00+08:00"
    Date_3_end_timestamp = "2023-11-02T18:10:00+08:00"

    # Extract the date component from the timestamps
    Date_1 = datetime.fromisoformat(Date_1_start_timestamp).strftime('%d-%m-%Y')
    Date_2 = datetime.fromisoformat(Date_2_start_timestamp).strftime('%d-%m-%Y')
    Date_3 = datetime.fromisoformat(Date_3_start_timestamp).strftime('%d-%m-%Y')

    current_date = datetime.now().date()

    # Compare the current date with the extracted dates
    if current_date == Date_1:
        print("The current date matches Date 1")
        answered_records_1, unanswered_records_1 = query_table(db_client, TABLE_NAME_1, start_timestamp=Date_1_start_timestamp, end_timestamp=Date_1_end_timestamp) # Make sure the time is correct

        Date_1_df1 = answered_calls(answered_records_1)
        Date_1_df2 = unanswered_calls(unanswered_records_1)

        clean_data(db_resource, s3_client, Date_1_df1, Date_1_df2,Date_1,Date_2,Date_3,0)

    elif current_date == Date_2:
        print("The current date matches Date 2")
        answered_records_2, unanswered_records_2 = query_table(db_client, TABLE_NAME_1, start_timestamp=Date_2_start_timestamp, end_timestamp=Date_2_end_timestamp) # Make sure the time is correct
        Date_2_df1 = answered_calls(answered_records_2)
        Date_2_df2 = unanswered_calls(unanswered_records_2)

        clean_data(db_resource, s3_client, Date_2_df1, Date_2_df2,Date_1,Date_2,Date_3,2)

    elif current_date == Date_3:
        print("The current date matches Date 3")

        answered_records_3, unanswered_records_3 = query_table(db_client, TABLE_NAME_1, start_timestamp=Date_3_start_timestamp, end_timestamp=Date_3_end_timestamp) # Make sure the time is correct

        Date_3_df1 =  answered_calls(answered_records_3)
        Date_3_df2 = unanswered_calls(unanswered_records_3)

        clean_data(db_resource, s3_client, Date_3_df1, Date_3_df2,Date_1,Date_2,Date_3,4)
    else:
        print("The current date does not match any of the provided dates.")