import os
import time
import datetime
import math
import json
import bson.json_util as json_util
from flask import Flask
from flask import request
from flask_bcrypt import Bcrypt
from flask_cors import CORS
from dotenv import load_dotenv
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
bcrypt = Bcrypt(app)
CORS(app)

#SERVER_IP_ADDR = os.getenv("SERVER_IP_ADDR")
#SERVER_PORT = os.environ["SERVER_PORT"]
ABC_MONGODB_URI = os.environ["ABC_MONGODB_URI"]
ABC_MONGODB_DB_NAME = os.environ["ABC_MONGODB_DB_NAME"]
ABC_MONGODB_COLLECTION = os.environ["ABC_MONGODB_COLLECTION"]
MEMBER_MONGODB_URI = os.environ["MEMBER_MONGODB_URI"]
MEMBER_MONGODB_DB_NAME = os.environ["MEMBER_MONGODB_DB_NAME"]
MEMBER_MONGODB_COLLECTION = os.environ["MEMBER_MONGODB_COLLECTION"]
LOCATION_MONGODB_COLLECTION = os.environ["LOCATION_MONGODB_COLLECTION"]

DATATYPE = [
    { "name": "bluetooth" },
    { "name": "wifi" },
    { "name": "battery" },
    { "name": "data_traffic" },
    { "name": "device_event" },
    { "name": "message" },
    { "name": "call_log" },
    { "name": "installed_app" },
    { "name": "location" },
    { "name": "fitness" },
    { "name": "physical_activity" },
    { "name": "physical_activity_transition" },
    { "name": "survey" },
    { "name": "media" },
    { "name": "app_usage_event" },
    { "name": "notification" }
]
TIMEZONE_OFFSET = 9
INTERVAL_BETWEEN_LOCATION_RECORDS = 11 * 60 * 1000

# return the distance between 2 points in km
def calDistance(lat1, lon1, lat2, lon2):
    # Radius of the Earth in km
    radius = 6371
    # Convert latitude and longitude to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    # Distance in km
    distance = radius * c
    return distance

# return ts array for time when user exists in target region
def getTSfromLocation(locationRecord, targetLat, targetLong, targetRadius):
    targetRadius = int(targetRadius) / 1000
    tsArray = []
    for i, loc in enumerate(locationRecord):
        tsArrayObj = {"startTS": 0, "endTS": 0}
        # within deletion region
        if(calDistance(loc["latitude"], loc["longitude"], targetLat, targetLong) <= targetRadius):
            # first entry
            if i == 0:
                tsArrayObj["startTS"] = loc["timestamp"]
                tsArray.append(tsArrayObj)
            # all other non-continuous entries
            if i > 0 and loc["timestamp"] - locationRecord[i - 1]["timestamp"] >= INTERVAL_BETWEEN_LOCATION_RECORDS:
                tsArray[len(tsArray) - 1]["endTS"] = locationRecord[i - 1]["timestamp"]
                tsArrayObj["startTS"] = loc["timestamp"]
                tsArray.append(tsArrayObj)
        # outside of deletion region
        else:
            if len(tsArray) > 0 and i > 0:
                tsArray[len(tsArray) - 1]["endTS"] = locationRecord[i - 1]["timestamp"]
                tsArrayObj["startTS"] = loc["timestamp"]
                tsArray.append(tsArrayObj)
    # set the last ts of tsArray as last entry in Location Member DB
    if len(tsArray) > 0:
        tsArray[len(tsArray) - 1]["endTS"] = locationRecord[len(locationRecord) - 1]["timestamp"]
    return tsArray

def tryScheduler():
    print("[Flask server.py] running scheduler at " + time.ctime())
    memberClient = MongoClient(MEMBER_MONGODB_URI)
    memberDB = memberClient[MEMBER_MONGODB_DB_NAME]
    memberDatum = memberDB[MEMBER_MONGODB_COLLECTION]
    locationDatum = memberDB[LOCATION_MONGODB_COLLECTION]
    ABCClient = MongoClient(ABC_MONGODB_URI)
    ABCDB = ABCClient[ABC_MONGODB_DB_NAME]
    ABCDatum = ABCDB[ABC_MONGODB_COLLECTION]
    user = list(memberDatum.find({}))
    # for each user
    for u in user:
        # for each datatype's status
        for s in u["status"]:
            # handle time filtering
            if u["status"][s] == "time":
                print("[Flask server.py] Handling time filtering for", s, "from user", u["email"])
                currentTime = int(time.time() * 1000)
                startingTime = datetime.datetime.strptime(u["timeFiltering"][s]["startingTime"], '%Y-%m-%dT%H:%M:%S.%fZ')
                endingTime = datetime.datetime.strptime(u["timeFiltering"][s]["endingTime"], '%Y-%m-%dT%H:%M:%S.%fZ')
                applyTime = u["timeFiltering"][s]["applyTS"]
                applyTime = applyTime - applyTime % (24 * 60 * 60 * 1000) - (TIMEZONE_OFFSET * 60 * 60 * 1000)
                # loop for each day to delete data on ABCLogger DB
                for t in range(applyTime, currentTime, 24 * 60 * 60 * 1000):
                    if startingTime.hour + TIMEZONE_OFFSET > 23:
                        startTS = t + (startingTime.hour + TIMEZONE_OFFSET - 24) * 60 * 60 * 1000 + startingTime.minute * 60 * 1000
                    else:
                        startTS = t + (startingTime.hour + TIMEZONE_OFFSET) * 60 * 60 * 1000 + startingTime.minute * 60 * 1000
                    if endingTime.hour + TIMEZONE_OFFSET > 23:
                        endTS = t + (endingTime.hour + TIMEZONE_OFFSET - 24) * 60 * 60 * 1000 + endingTime.minute * 60 * 1000
                    else:
                        endTS = t + (endingTime.hour + TIMEZONE_OFFSET) * 60 * 60 * 1000 + endingTime.minute * 60 * 1000
                    query = {
                        "$and": [
                            {
                                "subject.email": u["email"],
                                "datumType": s.upper()
                            },  
                            {
                                "timestamp": {"$gt": startTS}
                            },
                            {
                                "timestamp": {"$lt": endTS}
                            }
                        ]
                    }
                    deletion = ABCDatum.delete_many(query)
                    print(deletion.deleted_count)
            # handle location filtering
            if u["status"][s] == "location":
                print("[Flask server.py] Handling location filtering for", s, "from user", u["email"])
                targetLat = u["locationFiltering"][s]["latitude"]
                targetLong = u["locationFiltering"][s]["longitude"]
                targetRadius = u["locationFiltering"][s]["radius"]
                print("Targeting on", targetLat, targetLong, "for data type", s)
                query = {
                        "$and": [
                            {
                                "email": u["email"]
                            },  
                            {
                                "timestamp": {"$gt": u["locationFiltering"][s]["applyTS"]}
                            },
                            {
                                "timestamp": {"$lt": int(time.time() * 1000)}
                            }
                        ]
                    }
                locationRecord = list(locationDatum.find(query))
                # get the time ranges for deletion
                tsArray = getTSfromLocation(locationRecord, targetLat, targetLong, targetRadius)
                # delete the data within the ts on ABCLogger DB
                for ts in tsArray:
                    print(ts["startTS"], ts["endTS"])
                    query = {
                        "$and": [
                            {
                                "subject.email": u["email"],
                                "datumType": s.upper()
                            },  
                            {
                                "timestamp": {"$gt": ts["startTS"]}
                            },
                            {
                                "timestamp": {"$lt": ts["endTS"]}
                            }
                        ]
                    }
                    deletion = ABCDatum.delete_many(query)
                    print(deletion.deleted_count)
    memberClient.close()
    ABCClient.close()
    return

# tryScheduler()

# scheduler = BackgroundScheduler(daemon = True, timezone="Asia/Seoul")
# scheduler.add_job(tryScheduler, 'cron', hour=2, minute=12, misfire_grace_time=3600)
# scheduler.start()
# print("[Flask server.py] Scheduler set")

# delete data from MongoDB which matches the condition
@app.route("/deletedata", methods=['POST'])
def dataDeletion():
    print("[Flask server.py] POST path /deletedata")
    print("[Flask server.py] Process delete with filter " + str(request.json["queryFilter"]))
    # MongoDB connection
    client = MongoClient(ABC_MONGODB_URI)
    db = client[ABC_MONGODB_DB_NAME]
    datum = db[ABC_MONGODB_COLLECTION]
    query = {
        "$and": [request.json["queryFilter"]]
        }
    res = datum.delete_many(query)
    print("[Flask server.py] Deleted " + str(res.deleted_count) + " row(s) of data")
    client.close()
    return { "result": "deletedata" }

# fetch member data from PrivacyViz-Member MongoDB for login check
@app.route("/login", methods=['POST'])
def login():
    print("[Flask server.py] POST path /login")
    client = MongoClient(MEMBER_MONGODB_URI)
    db = client[MEMBER_MONGODB_DB_NAME]
    datum = db[MEMBER_MONGODB_COLLECTION]
    user = datum.find_one({"email": request.json["email"]})
    client.close()
    if(user):
        if(bcrypt.check_password_hash(user["password"], request.json["password"])):
            return { "result": True }
    return { "result": False }

# create entry in PrivacyViz-Member MongoDB
@app.route("/createuser", methods=['POST'])
def createUser():
    print("[Flask server.py] POST path /createuser")
    client = MongoClient(MEMBER_MONGODB_URI)
    db = client[MEMBER_MONGODB_DB_NAME]
    datum = db[MEMBER_MONGODB_COLLECTION]
    initStatus = {}
    initTimeFiltering = {}
    initLocationFiltering = {}
    for dt in DATATYPE:
        initStatus[dt['name']] = "on"
        initTimeFiltering[dt['name']] = {}
        initLocationFiltering[dt['name']] = {}
    duplicant = datum.find_one({"email": request.json["email"]})
    if(duplicant):
        client.close()
        return { "result": False }
    datum.insert_one({"email": request.json["email"], "password": bcrypt.generate_password_hash(request.json["password"]), "status": initStatus, "timeFiltering": initTimeFiltering, "locationFiltering": initLocationFiltering})
    client.close()
    return { "result": True }

# fetch status data from PrivacyViz-Member MongoDB for a specific user
@app.route("/status", methods=['POST'])
def getStatus():
    print("[Flask server.py] POST path /status")
    client = MongoClient(MEMBER_MONGODB_URI)
    db = client[MEMBER_MONGODB_DB_NAME]
    datum = db[MEMBER_MONGODB_COLLECTION]
    user = datum.find_one({"email": request.json["email"]})
    client.close()
    if(user):
        return user["status"]
    return {}

# update status data from PrivacyViz-Member MongoDB for a specific user
@app.route("/setstatus", methods=['POST'])
def setStatus():
    print("[Flask server.py] POST path /setstatus")
    client = MongoClient(MEMBER_MONGODB_URI)
    db = client[MEMBER_MONGODB_DB_NAME]
    datum = db[MEMBER_MONGODB_COLLECTION]
    user = datum.find_one_and_update({ "email": request.json["email"] }, { '$set': request.json["newStatus"] })
    client.close()
    if(user):
        return { "result": True }
    return {"result": False }

# fetch filtering setting data from PrivacyViz-Member MongoDB for a specific user
@app.route("/getfiltering", methods=['POST'])
def getFiltering():
    print("[Flask server.py] POST path /getfiltering")
    client = MongoClient(MEMBER_MONGODB_URI)
    db = client[MEMBER_MONGODB_DB_NAME]
    datum = db[MEMBER_MONGODB_COLLECTION]
    user = datum.find_one({ "email": request.json["email"] })
    client.close()
    if(user):
        return { "timeFiltering": user["timeFiltering"], "locationFiltering": user["locationFiltering"]}
    return { "timeFiltering": [], "locationFiltering": [] }

# save location record in PrivacyViz-Member MongoDB
@app.route("/locationrecord", methods=['POST'])
def saveLocationRecord():
    print("[Flask server.py] POST path /locationrecord")
    client = MongoClient(MEMBER_MONGODB_URI)
    db = client[MEMBER_MONGODB_DB_NAME]
    datum = db[LOCATION_MONGODB_COLLECTION]
    datum.insert_one(request.json["locationRecord"])
    client.close()
    return { "result": True }

# fetch data from MongoDB with the provided query filter
@app.route("/data", methods=['POST'])
def dataQuery():
    print("[Flask server.py] POST path /data")
    # init variable form post request
    email = request.json["email"]
    dataType = request.json["dataType"]
    date = request.json["date"]
    timeRange = request.json["timeRange"]
    # connection config
    memberClient = MongoClient(MEMBER_MONGODB_URI)
    memberDB = memberClient[MEMBER_MONGODB_DB_NAME]
    memberDatum = memberDB[MEMBER_MONGODB_COLLECTION]
    locationDatum = memberDB[LOCATION_MONGODB_COLLECTION]
    ABCClient = MongoClient(ABC_MONGODB_URI)
    ABCDB = ABCClient[ABC_MONGODB_DB_NAME]
    ABCDatum = ABCDB[ABC_MONGODB_COLLECTION]
    user = memberDatum.find_one({"email": email})
    # query for all data within time range
    query = {
        "$and": [
            {
                "subject.email": email,
                "datumType": dataType.upper()
            },  
            {
                "timestamp": {"$gt": date + timeRange[0]}
            },
            {
                "timestamp": {"$lt": date + timeRange[1]}
            }
        ]
    }
    res = list(ABCDatum.find(query))
    ABCClient.close()
    # filter out time specified in PrivacyViz-Member MongoDB under time filtering
    if user["status"][dataType] == "time":
        print("[Flask server.py] Should handle time filtering for", dataType)
        applyTime = user["timeFiltering"][dataType]["applyTS"]
        # handle filter starting time
        startingTime = datetime.datetime.strptime(user["timeFiltering"][dataType]["startingTime"], '%Y-%m-%dT%H:%M:%S.%fZ')
        if startingTime.hour + TIMEZONE_OFFSET > 23:
            filterStartTS = date + (startingTime.hour + TIMEZONE_OFFSET - 24) * 60 * 60 * 1000 + startingTime.minute * 60 * 1000
        else:
            filterStartTS = date + (startingTime.hour + TIMEZONE_OFFSET) * 60 * 60 * 1000 + startingTime.minute * 60 * 1000
        # handle filter ending time
        endingTime = datetime.datetime.strptime(user["timeFiltering"][dataType]["endingTime"], '%Y-%m-%dT%H:%M:%S.%fZ')
        if endingTime.hour + TIMEZONE_OFFSET > 23:
            filterEndTS = date + (endingTime.hour + TIMEZONE_OFFSET - 24) * 60 * 60 * 1000 + startingTime.minute * 60 * 1000
        else:
            filterEndTS = date + (endingTime.hour + TIMEZONE_OFFSET) * 60 * 60 * 1000 + startingTime.minute * 60 * 1000
        # filter out time + close conn + return
        filtered_list = []
        filtered_list = [r for r in res if (r['timestamp'] < filterStartTS or r['timestamp'] > filterEndTS) or r['timestamp'] < applyTime]
        memberClient.close()
        return json.loads(json_util.dumps({"res": filtered_list}))
    # filter out location specified in PrivacyViz-Member MongoDB under location filtering
    elif user["status"][dataType] == "location":
        print("[Flask server.py] Should handle location filtering for", dataType)
        # get location filtering detail from PrivacyViz-Member MongoDB
        targetLat = user["locationFiltering"][dataType]["latitude"]
        targetLong = user["locationFiltering"][dataType]["longitude"]
        targetRadius = user["locationFiltering"][dataType]["radius"]
        applyTime = user["locationFiltering"][dataType]["applyTS"]
        # get all location information from PrivacyViz-Member MongoDB Location collection
        query = {
            "$and": [
                {
                    "email": email,
                },  
                {
                    "timestamp": {"$gt": date + timeRange[0]}
                },
                {
                    "timestamp": {"$lt": date + timeRange[1]}
                }
            ]
        }
        locationRecord = list(locationDatum.find(query))
        # get the time ranges for deletion
        tsArray = getTSfromLocation(locationRecord, targetLat, targetLong, targetRadius)
        # filter out the data within the ts
        if len(tsArray) == 0:
            filtered_list = res
        else:
            for ts in tsArray:
                filtered_list = [r for r in res if (r['timestamp'] < ts["startTS"] or r['timestamp'] > ts["endTS"]) or r['timestamp'] < applyTime]
                res = [r for r in res if r['timestamp'] > ts["endTS"]]
        # close conn + return
        memberClient.close()
        return json.loads(json_util.dumps({"res": filtered_list}))
    # no need filtering for show all, just close conn + return
    else:
        memberClient.close()
        return json.loads(json_util.dumps({"res": res}))

# test Flask server + PrivacyViz-Member MongoDB connection
@app.route("/test", methods=['GET'])
def testConnection():
    print("[Flask server.py] GET path /test")
#     client = MongoClient(MEMBER_MONGODB_URI)
#     db = client[MEMBER_MONGODB_DB_NAME]
#     datum = db[MEMBER_MONGODB_COLLECTION]
#     user = datum.find_one({"email": "1@1"})
#     if(user):
#         return {"result": user["email"]}
#     print("Nothing")
#     return { "result": False }
    client = MongoClient(ABC_MONGODB_URI)
    db = client[ABC_MONGODB_DB_NAME]
    datum = db[ABC_MONGODB_COLLECTION]
    query = {
        "$and": [
            {
                "subject.email": 'emily@kse.kaist.ac.kr',
                "datumType": "BLUETOOTH"
            },  
            {
                "timestamp": {"$gt": 1682592316687} # Data after 01/04/2023
            },
            {
                "timestamp": {"$lt": 1682635521699}
            }
        ]
    }
    res = list(datum.find(query))
    client.close()
    if(res):
        print("[Flask server.py] First entry of fetched data:" , len(res))
    else:
        print("[Flask server.py] No data is founded")
        return { "result": "ConnSucess but nothing found" }
    return { "result": "ConnSuccess" }

# test the background running function in RN
@app.route("/testbackground", methods=['POST'])
def testBackground():
    print("[Flask server.py] POST path /testbackground")
    print(request.json["body"])
    return { "result": True }


@app.route("/conn1", methods=['GET'])
def func1():
    print("[Flask server.py] GET path /conn1")
    return { "result": True }

@app.route("/conn2", methods=['GET'])
def func2():
    print("[Flask server.py] GET path /conn2")
    return { "result": ABC_MONGODB_URI }
