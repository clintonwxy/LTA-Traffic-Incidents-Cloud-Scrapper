import requests
import pandas as pd
import datetime
import boto3
import json


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
    data = request.json()
    dt = datetime.datetime.now() + datetime.timedelta(hours=8) # Adjsuted to Singapore time

    df = pd.DataFrame(data["value"])

    if (len(df) > 0):
        
        print("Yup there are files found in the api")

        # 1. Extract Date and Time out of the Message and keep the main message only
        date_regex = "([0-9]{1,2}/[0-9]{1,2})"
        df["Date"] = df["Message"].str.extract(pat = date_regex) + "/" + str(dt.year)

        time_regex = "([0-9]{1,2}:[0-9]{1,2})"
        df["Time"] = df["Message"].str.extract(pat = time_regex)

        df["Date_Time_Start"] = pd.to_datetime(df["Date"] + df["Time"], format = "%d/%m/%Y%H:%M").astype(str)

        message_regex = "\d\s(.*$)"
        df["Message"] = df["Message"].str.extract(pat = message_regex)
        
        df["Date_Time_End"] = "Nil"

        df = df[["Type", "Date_Time_Start", "Date_Time_End", "Message", "Latitude", "Longitude"]]

        # 2. Extract temp file
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('cloud_project')  # Name of my DynamoDB

        temp = table.get_item(Key = {"date_time" : "temp"})

        print("temp==========================")
        print(temp)
        print("df============================")
        print(df)
        print("==============================")

        try:

            print("trying to do the sorting out 21...")

            temp = temp["Item"]["value"]
            temp = pd.DataFrame(json.loads(temp))

            df["key"] = df["Type"] + " " + df["Message"]
            temp["key"] = temp["Type"] + " " + temp["Message"]

            check_active = temp["key"].isin(df["key"])

            temp = temp[~check_active] #Inverse to keep only those that are expired, i.e. incident is resolved and is not active
            temp["Date_Time_End"] = dt.strftime(format = "%Y-%m-%d %H:%M:%S")
            temp = temp[["Type", "Date_Time_Start", "Date_Time_End", "Message", "Latitude", "Longitude"]]
            
            # Key for Dynamo DB
            df_tocloud_key_insert = dt.strftime(format = "%Y-%m-%d %H:%M")
            df_tocloud_insert = temp

            print("the key is: " + df_tocloud_key_insert)
            print("inserted is:")
            print(df_tocloud_insert)

            send_to_dynamodb_insert = {
                "date_time": df_tocloud_key_insert,
                "value": df_tocloud_insert.to_json()
            }

            table.put_item(Item=send_to_dynamodb_insert)

            print("inserted new file successfully")

            df_tocloud_key_temp = "temp"
            df_tocloud_temp = df

            print("the key is:" + df_tocloud_key_temp)
            print("temp is: ")
            print(df_tocloud_temp)

            send_to_dynamodb_temp = {
                "date_time": df_tocloud_key_temp,
                "value": df_tocloud_temp.to_json()
            }

            table.put_item(Item=send_to_dynamodb_temp)

            print("temp added successfully")

        except:

            print("No temp") #temp does not exist. Send df to DynamoDB as temp

            # Key for DynamoDB
            df_tocloud_key_temp = "temp"
            df_tocloud_temp = df

            if(len(df_tocloud_temp) > 0):

                send_to_dynamodb_temp = {
                    "date_time": df_tocloud_key_temp,
                    "value": df_tocloud_temp.to_json()
                }

                table.put_item(Item=send_to_dynamodb_temp)
            
            print("The key is: " + df_tocloud_key_temp)
            print("The total incidents are: " + str(len(df_tocloud_temp)))
            
    else:

        print("No data from API, starting to close all active temp files...")

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('cloud_project')  # Name of my DynamoDB

        temp = table.get_item(Key = {"date_time" : "temp"})

        print("temp attempted to retrieve successfully")

        try:
            print("trying to check if there are temp info.....")
            temp = temp["Item"]["value"]
            temp = pd.DataFrame(json.loads(temp))

            temp["Date_Time_End"] = dt.strftime(format = "%Y-%m-%d %H:%M:%S")
            temp = temp[["Type", "Date_Time_Start", "Date_Time_End", "Message", "Latitude", "Longitude"]]
            
            print("parsed temp file successfully 23")

            # Key for Dynamo DB
            df_tocloud_key_insert = dt.strftime(format = "%Y-%m-%d %H:%M")
            df_tocloud_insert = temp

            print("The key is:" + df_tocloud_key_insert)
            print("The closed file is:")
            print(df_tocloud_insert)

            send_to_dynamodb_insert = {
                "date_time": df_tocloud_key_insert,
                "value": df_tocloud_insert.to_json()
            }

            table.put_item(Item=send_to_dynamodb_insert)

            print("successfully added to server 44")

            table.delete_item(Key = {"date_time" : "temp"})

            print("successfully deteleted temp from server 67")


        except:
            print("No data from API. No temp file")