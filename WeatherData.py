# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 11:38:01 2017

@author: aga
"""

import requests
import json
import datetime
import pytz
import DataCollectUtil as util
import xml.etree.ElementTree as ET

class WeatherData:
    def __init__(self, connection, key):
        self.connection = connection
        self.key = key
        
    def CreateTable(self):
        print("Create Table for Weather Data")
        with self.connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS WeatherStations (\
                id VARCHAR(255),\
                name VARCHAR(255),\
                lat FLOAT,\
                lng FLOAT,\
                city VARCHAR(255),\
                town VARCHAR(255),\
                PRIMARY KEY (id)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS WeatherData (\
                stationID VARCHAR(255),\
                height FLOAT,\
                wDir INT,\
                wSpeed FLOAT,\
                t FLOAT,\
                h FLOAT,\
                p FLOAT,\
                sum FLOAT,\
                rain FLOAT,\
                time DATETIME,\
                PRIMARY KEY (stationID,time),\
                INDEX(stationID),\
                INDEX(time),\
                );"
            cursor.execute(sql)

        self.connection.commit()
        
    def CollectData(self):
        print("Collect Weather Data")
        r = requests.get("http://opendata.cwb.gov.tw/opendataapi?dataid=O-A0001-001&authorizationkey="+self.key)
        #r.encoding = "utf-8"
        if r.status_code == requests.codes.all_okay:
            root = ET.fromstring(r.text)
            pos = root.tag.find("}")
            ns = root.tag[:pos+1]
            
            stationArr = []
            dataArr = []
            for location in root.findall(ns+'location'):
                station = {}
                station["id"] = location.find(ns+"stationId").text
                station["name"] = location.find(ns+"locationName").text
                station["lat"] = location.find(ns+"lat").text
                station["lng"] = location.find(ns+"lon").text
                for param in location.findall(ns+'parameter'):
                    if(param[0].text == "CITY"):
                        station["city"] = param[1].text
                    elif(param[0].text == "TOWN"):
                        station["town"] = param[1].text
                stationArr.append(station)
            
                data = {}
                data["time"] = location.find(ns+"time").find(ns+"obsTime").text
                data["stationID"] = station["id"]
                for elem in location.findall(ns+"weatherElement"):
                    if(elem[0].text == "ELEV"):
                        data["height"] = elem[1][0].text
                    elif(elem[0].text == "WDIR"):
                        data["wDir"] = elem[1][0].text
                    elif(elem[0].text == "WDSD"):
                        data["wSpeed"] = elem[1][0].text
                    elif(elem[0].text == "TEMP"):
                        data["t"] = elem[1][0].text
                    elif(elem[0].text == "HUMD"):
                        data["h"] = elem[1][0].text
                    elif(elem[0].text == "PRES"):
                        data["p"] = elem[1][0].text
                    elif(elem[0].text == "SUN"):
                        data["sum"] = elem[1][0].text
                    elif(elem[0].text == "H_24R"):
                        data["rain"] = elem[1][0].text
                dataArr.append(data)
  
            field = "id,name,lat,lng,city,town"
            keyStr = "id,name,lat,lng,city,town"
            for station in stationArr:
                val = util.GenValue(station,keyStr)
                with self.connection.cursor() as cursor:
                    sql = "INSERT IGNORE INTO WeatherStations ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                    
            field = "stationID,height,wDir,wSpeed,t,h,p,sum,rain,time"
            keyStr = "stationID,height,wDir,wSpeed,t,h,p,sum,rain,time"
            for data in dataArr:
                val = util.GenValue(data,keyStr)
                with self.connection.cursor() as cursor:
                    sql = "INSERT IGNORE INTO WeatherData ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                
            self.connection.commit()
            
        
        