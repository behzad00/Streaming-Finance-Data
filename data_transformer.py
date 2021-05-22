import json
import yfinance as yf
import boto3
import datetime
import time

kinesis = boto3.client('kinesis', "us-east-1")
def lambda_handler(event, context):
    # TODO implement
    stock    = ['FB', 'SHOP', 'BYND','NFLX','PINS','SQ','TTD','OKTA','SNAP','DDOG']
    # stock = ['DDOG']
    ts_start = datetime.datetime(2021, 5, 11, 9, 0, 0, 0)
    ts_end   = datetime.datetime(2021, 5, 11, 23, 59, 0, 0)
#    ts_start = '2021-05-11'
#    ts_end   = '2021-05-12'
    
    data_list = []
    
    for s in stock:
        record =yf.Ticker(s)
        hist = record.history(interval = '5m', start=ts_start, end=ts_end)
        #print(hist)
        hist.reset_index(inplace=True)
        for index, row in hist.iterrows():
#        for row in hist.index:
#            print(row)
            current_row = {"high": row["High"], "low": row["Low"] , "ts": str(row["Datetime"]) , "name": s}
            print(row["Datetime"], s)
            data = json.dumps(current_row)+"\n"
            data_list.append(data)
            kinesis.put_record(
                StreamName="thursday20",
                Data=data,
                PartitionKey="partitionkey")
    #print(data_list)
    return {
        'statusCode': 200,
#       'body': hist[["Date", "Open", "High", "Low"]].to_json()
        'body': data
    }