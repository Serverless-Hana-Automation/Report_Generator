import boto3
import pandas as pd
import xlsxwriter
from datetime import datetime
from numpy import nan
import os 


TABLE_NAME= os.environ["TABLE_NAME"]
REGION= os.environ["REGION"]
BUCKET_NAME= os.environ["BUCKET_NAME"]
TABLE_NAME_2= os.environ["TABLE_NAME_2"]

dynamodb = boto3.client("dynamodb",region_name=REGION)
s3 = boto3.client("s3",region_name= REGION)

bucket_name = BUCKET_NAME
object_key_1 = "unanswered/schedule_call.xlsx"
object_key_2 = "unanswered/daily_report.xlsx"

def query_table(start_timestamp, end_timestamp):
  # QUERY ANSWERED CALLS
  answered_response_items = []
  answered_response = dynamodb.query(
      TableName= TABLE_NAME,
      IndexName="Call_Answered-index",
      ExpressionAttributeValues={
          ":ca": {"S": "True"},
          ":start_ts": {"S": start_timestamp},
          ":end_ts": {"S": end_timestamp}
      },
      KeyConditionExpression="Call_Answered = :ca",
      FilterExpression="Trigger_Timestamp BETWEEN :start_ts AND :end_ts",
  )
  answered_response_items.extend(answered_response.get('Items', []))

  # Continue querying while paginated results exist
  while 'LastEvaluatedKey' in answered_response:
      answered_response = dynamodb.query(
          TableName="Agent_Trigger_Table_PreProd",
          IndexName="Call_Answered-index",
          ExpressionAttributeValues={
              ":ca": {"S": "True"},
              ":start_ts": {"S": start_timestamp},
              ":end_ts": {"S": end_timestamp}
          },
          KeyConditionExpression="Call_Answered = :ca",
          FilterExpression="Trigger_Timestamp BETWEEN :start_ts AND :end_ts",
          ExclusiveStartKey=answered_response['LastEvaluatedKey']
      )
      answered_response_items.extend(answered_response.get('Items', []))

  # QUERY UNANSWERED CALLS
  unanswered_response_items = []
  unanswered_response = dynamodb.query(
      TableName="Agent_Trigger_Table_PreProd",
      IndexName="Call_Answered-index",
      ExpressionAttributeValues={
          ":ca": {"S": "False"},
          ":start_ts": {"S": start_timestamp},
          ":end_ts": {"S": end_timestamp}
      },
      KeyConditionExpression="Call_Answered = :ca",
      FilterExpression="Trigger_Timestamp BETWEEN :start_ts AND :end_ts",
  )
  unanswered_response_items.extend(unanswered_response.get('Items', []))

  # Continue querying while paginated results exist
  while 'LastEvaluatedKey' in unanswered_response:
      unanswered_response = dynamodb.query(
          TableName="Agent_Trigger_Table_PreProd",
          IndexName="Call_Answered-index",
          ExpressionAttributeValues={
              ":ca": {"S": "False"},
              ":start_ts": {"S": start_timestamp},
              ":end_ts": {"S": end_timestamp}
          },
          KeyConditionExpression="Call_Answered = :ca",
          FilterExpression="Trigger_Timestamp BETWEEN :start_ts AND :end_ts",
          ExclusiveStartKey=unanswered_response['LastEvaluatedKey']
      )
      unanswered_response_items.extend(unanswered_response.get('Items', []))

  return answered_response_items, unanswered_response_items


def answered_calls(response_items):
  answered_response_df = []

  for item in response_items:
    policy_number = item.get('Policy_Number', {}).get('S', None)
    if policy_number.__contains__("HANATEST"):
      continue # Skip rows with "HANATEST"
    last_stage = next(iter(item.get('Bot_Comprehensibility', {})["L"][-1]["M"]))
    entity = classify_entity(item.get('Policy_Number', {}).get('S', ''))
    verification_status = classify_verification_status(item.get('Verification', []))
    policy_received = classify_policy_received(item.get('Policy_Received'))
    survey_rating = classify_survey_rating(item.get('Survey_Rating'))
    hana_call_time = item.get('Trigger_Timestamp', {}).get('S', None)
    phone_number = dynamodb.query(
        TableName= TABLE_NAME_2,
        ExpressionAttributeValues={':pn' : {'S' : policy_number}},
        KeyConditionExpression="Policy_Number = :pn",
        ProjectionExpression="Policyholder_Phone_Number",
      )
    phone_number = phone_number.get('Items')[0].get('Policyholder_Phone_Number', {}).get('S', '')

    answered_response_df.append({
        "Policy_Number": policy_number,
        "Entity": entity,
        "Phone_Number": phone_number,
        "HANA Call Time": hana_call_time,
        "Verification": verification_status,
        "Policy Received": policy_received,
        "Survey Rating": survey_rating,
        "Last Stage": last_stage,
    })

  # Create DataFrame from the list of items
  answered_df = pd.DataFrame(answered_response_df)

  return answered_df


def unanswered_calls(response_items):
  response_df = []
  for item in response_items:
    unanswered_policy_number = item.get('Policy_Number', {}).get('S', None)
    if unanswered_policy_number.__contains__("HANATEST"):
      continue # Skip rows with "HANATEST"

    unanswered_phone_number = dynamodb.query(
      TableName= TABLE_NAME_2,
      ExpressionAttributeValues={':pn' : {'S' : unanswered_policy_number}},
      KeyConditionExpression="Policy_Number = :pn",
      ProjectionExpression="Policyholder_Phone_Number",
    )
    unanswered_phone_number = unanswered_phone_number.get('Items')[0].get('Policyholder_Phone_Number', {}).get('S', '')

    entity = classify_entity(unanswered_policy_number)

    response_df.append({
          "Policy_Number": unanswered_policy_number,
          "Entity": entity,
          "Phone_Number": unanswered_phone_number,
      })
  df2 = pd.DataFrame(response_df)
  df2 = df2.drop_duplicates("Policy_Number", keep='first')

  return df2

def classify_entity(policy_number):
    if policy_number.startswith("TR") or policy_number.startswith("LR"):
        return "FTA"
    else:
        return "LIA"


def classify_verification_status(verification):
    if not verification:
        return "NA"
    last_key = next(iter(verification["L"][-1]["M"]))
    return "PASSED" if verification["L"][-1]["M"][last_key]["S"] == "True" else "FAILED"


def classify_policy_received(policy_received):
    if not policy_received:
        return "Did not reach the stage"
    return "YES" if policy_received["S"] == "True" else "NO"


def classify_survey_rating(survey_rating):
    if not survey_rating:
        return "Not applicable"
    return [{key: value['S']} for ratings in survey_rating["L"] for key, value in ratings['M'].items()]

def clean_data(df1, df2, df3):
  stages = ["NA",nan, 'T.1', 'F.1', '1.1', '1.2', '1.3', '2.1', '2.2', '2.3', '3.1', '3.2', '4.1', '4.2', '4.3', '5.1', '5.2', '5.3', '5.4', '5.5', '6.1']
  ranking_dict = {i: stages.index(i) for i in stages}
  df1.loc[:, "Stages_Reached"] = df1["Last Stage"].apply(lambda x: ranking_dict[x])
  df1 = df1.sort_values(by=["HANA Call Time", "Stages_Reached"],ascending=False)
  df1 = df1.drop_duplicates(subset=["Policy_Number"],keep="first")

  filtered_df1 = df1[df1['Last Stage'] == 'T.1']
  filtered_df1 = filtered_df1[['Policy_Number', 'Entity', 'Phone_Number']]
  df1.drop(filtered_df1.index, inplace=True)

  df1['HANA Call Time'] = pd.to_datetime(df1['HANA Call Time'])
  df1['HANA Call Time'] = df1['HANA Call Time'].dt.strftime('%H:%M')

  df1 = df1.sort_values(by=["HANA Call Time"],ascending=True)
  df1 = df1.drop(columns=["Stages_Reached"],axis=1)

  concatenated_unanswered_df = pd.concat([df2, filtered_df1], axis=0)
  filtered_unanswered = concatenated_unanswered_df[~concatenated_unanswered_df["Policy_Number"].isin(df1["Policy_Number"])]
  clean_unanswered = filtered_unanswered.drop_duplicates('Policy_Number')

  # Write daily report to an Excel file
  with pd.ExcelWriter('/tmp/DailyReport.xlsx', engine='xlsxwriter') as writer:
      df1.to_excel(writer, sheet_name='Answered_Calls', index=False)
      clean_unanswered.to_excel(writer, sheet_name='Unanswered_Calls', index=False)

  #Create a file to schedule call
  schedule_call = clean_unanswered.drop(columns=["Entity"])
  schedule_call.to_excel("/tmp/schedule_call.xlsx", index=False)

  #Upload files to s3 for scheduling, and reporting
  s3.upload_file("/tmp/schedule_call.xlsx", bucket_name, object_key_1)
  s3.upload_file('/tmp/DailyReport.xlsx', bucket_name, object_key_2)

  print(f'Uploaded to {bucket_name}/{object_key_1} & {object_key_2}')