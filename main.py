from src.ReportScript import query_table, answered_calls, unanswered_calls, clean_data

def main(event, context):
  answered_records, unanswered_records = query_table(start_timestamp="2023-08-01T09:00:00+08:00", end_timestamp="2023-08-30T18:00:00+08:00")
  df1 = answered_calls(answered_records)
  df2 = unanswered_calls(unanswered_records)
  clean_data(df1, df2)
  
  return "function has been executed"