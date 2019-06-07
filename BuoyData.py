# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 11:02:14 2019

@author: aga
"""

import requests
import json
import datetime
import pytz
import DataCollectUtil as util
import xml.etree.ElementTree as ET
import math

class BuoyData:
    def __init__(self, connection, key):
        self.connection = connection
        self.key = key
        
    def CreateTable(self):
        print("Create Table for BuoyData")
        with self.connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS buoy (\
                stationId VARCHAR(255),\
                obsTime DATETIME,\
                gust_wind1 FLOAT,\
                wind1 FLOAT,\
                wdir1 FLOAT,\
                gust_wind2 FLOAT,\
                wind2 FLOAT,\
                wdir2 FLOAT,\
                pres FLOAT,\
                temp FLOAT,\
                sea_temp FLOAT,\
                wave_height FLOAT,\
                wave_period FLOAT,\
                wave_dir FLOAT,\
                PRIMARY KEY (stationID,obsTime),\
                INDEX(obsTime)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS buoy_tide (\
                stationId VARCHAR(255),\
                locationName VARCHAR(255),\
                obsTime DATETIME,\
                depth FLOAT,\
                sea_temp FLOAT,\
                PRIMARY KEY (stationID,obsTime),\
                INDEX(obsTime)\
                );"
            cursor.execute(sql)

        self.connection.commit()
        
    def CollectData(self):
        print("Collect BuoyData")
        r = requests.get("https://opendata.cwb.gov.tw/opendataapi?dataid=O-A0018-001&format=json&authorizationkey="+self.key)
        if r.status_code == requests.codes.all_okay:
            data = r.json()
            location = data["cwbopendata"]["dataset"]["location"]
            for loc in location:
                d = {}
                d["stationId"] = loc["stationId"]
                d["obsTime"] = loc["time"]["obsTime"]
                gust_wind_id = 1
                wind_id = 1
                wdir_id = 1
                for elem in loc["time"]["weatherElement"]:
                    if elem["elementName"] == "陣風":
                        d["gust_wind"+str(gust_wind_id)] = elem["elementValue"]["value"]
                        gust_wind_id += 1
                    elif elem["elementName"] == "平均風":
                        d["wind"+str(wind_id)] = elem["elementValue"]["value"]
                        wind_id += 1
                    elif elem["elementName"] == "風向":
                        d["wdir"+str(wdir_id)] = elem["elementValue"]["value"]
                        wdir_id += 1
                    elif elem["elementName"] == "氣壓":
                        d["pres"] = elem["elementValue"]["value"]
                    elif elem["elementName"] == "氣溫":
                        d["temp"] = elem["elementValue"]["value"]
                    elif elem["elementName"] == "海溫":
                        d["sea_temp"] = elem["elementValue"]["value"]
                    elif elem["elementName"] == "浪高":
                        d["wave_height"] = elem["elementValue"]["value"]
                    elif elem["elementName"] == "週期":
                        d["wave_period"] = elem["elementValue"]["value"]
                    elif elem["elementName"] == "波向":
                        d["wave_dir"] = elem["elementValue"]["value"]
                #print(d)
                util.DataToDB(self.connection,"buoy",d)
                
            self.connection.commit()
        
        
        r = requests.get("https://opendata.cwb.gov.tw/opendataapi?dataid=O-A0019-001&format=json&authorizationkey="+self.key)
        if r.status_code == requests.codes.all_okay:
            data = r.json()
            location = data["cwbopendata"]["dataset"]["location"]
            for loc in location:
                d = {}
                d["stationId"] = loc["stationId"]
                d["locationName"] = loc["locationName"]
                d["obsTime"] = loc["time"]["obsTime"]
                for elem in loc["time"]["weatherElement"]:
                    if elem["elementName"] == "深度":
                        d["depth"] = elem["elementValue"]["value"] or "NULL"
                    elif elem["elementName"] == "海溫":
                        d["sea_temp"] = elem["elementValue"]["value"] or "NULL"
                #print(d)
                util.DataToDB(self.connection,"buoy_tide",d)
                
            self.connection.commit()