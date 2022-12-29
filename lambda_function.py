import requests
import pandas as pd
import datetime
import boto3


def lambda_handler(event, context):
    """
    Code for AWS Lambda only, not on local machine

    The function works by requesting a json of the latest road incidents from the LTA Dynamic Data API. This lambda is queried hourly. After quering the data, 
    it checks if the api returns any data. If there is, it converts it into a pandas table, and creates the Date and Time column. It then filters out to keep the data
    from the past hourly, and checks again if there is any data. If there is, it converts it into a json table, and is inserted into the DynamoDB table.
    """

    # URL and Pulling Data
    traffic_url = "http://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents"
    api_key = open("api_key.txt").read()
    api_headers = {"AccountKey": api_key, "accept": "application/json"}

    request = requests.get(url=traffic_url, headers=api_headers)
    print(request)
    data = request.json()
    dt = datetime.datetime.now() + datetime.timedelta(hours=8)
    # Adding of timedelta only needed in AWS due to time difference, adjsuted to Singapore time

    df = pd.DataFrame(data["value"])

    if (len(df) > 0):

        # Extract Date and Time out of the Message and keep the main message only
        date_regex = "([0-9]{1,2}/[0-9]{1,2})"
        df["Date"] = df["Message"].str.extract(
            pat=date_regex) + "/" + str(dt.year)

        time_regex = "([0-9]{1,2}:[0-9]{1,2})"
        df["Time"] = df["Message"].str.extract(pat=time_regex)

        message_regex = "\d\s(.*$)"
        df["Message"] = df["Message"].str.extract(pat=message_regex)

        # Filtering to keep only previous hour of data
        if dt.hour >= 1:
            prev_hour = str(dt.hour - 1)
            df_boolean = df["Time"].str.match(pat=(prev_hour + ":"))
        else:
            prev_hour = "23"
            df_boolean = df["Time"].str.match(pat="23:")

        # Preparing Key and Value for Saving to DynamoDB
        if (int(prev_hour) == 23):
            df_tocloud_key = str(
                dt.date() - datetime.timedelta(days=1)) + " " + prev_hour
        else:
            df_tocloud_key = str(dt.date()) + " " + prev_hour.zfill(2)
        df_tocloud = df[df_boolean].reset_index(drop=True)

        # Adding to DynamoDB
        if (len(df_tocloud) > 0):

            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table('cloud_project')  # Name of my DynamoDB

            send_to_dynamodb = {
                "date_time": df_tocloud_key,
                "value": df_tocloud.to_json()
            }

            table.put_item(Item=send_to_dynamodb)

            # Log Data
            print("The key is: " + df_tocloud_key)
            print("The total incidents are: " + str(len(df_tocloud)))

        else:
            print("There are no incidents in the past hour")
