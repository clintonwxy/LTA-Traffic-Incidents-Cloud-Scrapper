import requests
import pandas as pd
from datetime import datetime

# URL and Pulling Data
traffic_url = "http://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents"
api_key = open("api_key.txt").read()
api_headers = {"AccountKey": api_key, "accept": "application/json"}

request = requests.get(url=traffic_url, headers=api_headers)
print(request)
data = request.json()

# Time References
dt = datetime.now()

dt_year = dt.year
dt_hour = dt.hour

df = pd.DataFrame(data["value"])

if (len(df) > 0):
    print()
    # Continue with code

# Extract Date and Time out of the Message to keep the main message only

date_regex = "([0-9]{1,2}/[0-9]{1,2})"
df["Date"] = df["Message"].str.extract(pat = date_regex) + "/" + str(dt.year)

time_regex = "([0-9]{1,2}:[0-9]{1,2})"
df["Time"] = df["Message"].str.extract(pat = time_regex)

message_regex = "\d\s(.*$)"
df["Message"] = df["Message"].str.extract(pat = message_regex)

# Previous Hour
if dt_hour >= 1:
    prev_hour = str(dt_hour - 1)
    df_boolean = df["Time"].str.match(pat = (prev_hour + ":"))
else:
    prev_hour = "23"
    df_boolean = df["Time"].str.match(pat = "23:")

df_tocloud = df[df_boolean].reset_index(drop = True)
print(df_tocloud)

df_tocloud_key = str(dt.date()) + " " + prev_hour
df_tocloud_key

print(df_tocloud_key)
