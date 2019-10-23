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
import math

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
                INDEX(time)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS cwb_DATA (\
                year int(4),\
                month int(4),\
                day int(2),\
                hour int(2),\
                date datetime,\
                station_id VARCHAR(255),\
                ELEV decimal(10,2),\
                WDIR decimal(10,2),\
                WDSD decimal(10,2),\
                TEMP decimal(10,2),\
                HUMD decimal(10,2),\
                PRES decimal(10,2),\
                24R decimal(10,2),\
                u decimal(10,5),\
                v decimal(10,5),\
                PRIMARY KEY (year,month,day,hour,date,station_id),\
                INDEX(station_id),\
                INDEX(date)\
                );"
            cursor.execute(sql)

            sql = "CREATE TABLE IF NOT EXISTS auto_rain_station (\
                stationId VARCHAR(255),\
                locationName VARCHAR(255),\
                lat FLOAT,\
                lon FLOAT,\
                CITY VARCHAR(255),\
                CITY_SN VARCHAR(255),\
                TOWN VARCHAR(255),\
                TOWN_SN VARCHAR(255),\
                ATTRIBUTE VARCHAR(255),\
                PRIMARY KEY (stationId)\
                );"
            cursor.execute(sql)

            sql = "CREATE TABLE IF NOT EXISTS auto_rain (\
                stationId VARCHAR(255),\
                time DATETIME,\
                ELEV FLOAT,\
                RAIN FLOAT,\
                MIN_10 FLOAT,\
                HOUR_3 FLOAT,\
                HOUR_6 FLOAT,\
                HOUR_12 FLOAT,\
                HOUR_24 FLOAT,\
                NOW FLOAT,\
                latest_2days FLOAT,\
                latest_3days FLOAT,\
                PRIMARY KEY (stationId,time),\
                INDEX(time)\
                );"
            cursor.execute(sql)

        self.connection.commit()
        
    def CollectData(self):
        print("Collect Weather Data")
        r = requests.get("https://opendata.cwb.gov.tw/opendataapi?dataid=O-A0001-001&authorizationkey="+self.key)
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
            
    def CollectDataNCHU(self):
        print("Collect Weather Data NCHU")
        r = requests.get("https://opendata.cwb.gov.tw/opendataapi?dataid=O-A0001-001&authorizationkey="+self.key)
        #r.encoding = "utf-8"
        if r.status_code == requests.codes.all_okay:
            root = ET.fromstring(r.text)
            pos = root.tag.find("}")
            ns = root.tag[:pos+1]
        
            stationArr = []
            dataArr = []
            for location in root.findall(ns+'location'):
                data = {}
                dateStr = location.find(ns+"time").find(ns+"obsTime").text
                dateStr = ''.join(dateStr.rsplit(':', 1))   #去掉時區的:
                dateObj = datetime.datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S%z")
                oneMinAgo = dateObj - datetime.timedelta(minutes=1)
                data["date"] = oneMinAgo.strftime('%Y-%m-%d %H:%M:%S')
                data["year"] = oneMinAgo.year
                data["month"] = oneMinAgo.month
                data["day"] = oneMinAgo.day
                data["hour"] = oneMinAgo.hour
                data["station_id"] = location.find(ns+"stationId").text
                for elem in location.findall(ns+"weatherElement"):
                    if(elem[0].text == "ELEV"):
                        data["ELEV"] = float(elem[1][0].text)
                    elif(elem[0].text == "WDIR"):
                        data["WDIR"] = float(elem[1][0].text)
                    elif(elem[0].text == "WDSD"):
                        data["WDSD"] = float(elem[1][0].text)
                    elif(elem[0].text == "TEMP"):
                        data["TEMP"] = float(elem[1][0].text)+ 273.15
                    elif(elem[0].text == "HUMD"):
                        data["HUMD"] = float(elem[1][0].text)*100
                    elif(elem[0].text == "PRES"):
                        data["PRES"] = float(elem[1][0].text)*100
                    elif(elem[0].text == "H_24R"):
                        data["24R"] = elem[1][0].text
                data["u"] = (-1)*data["WDSD"]*math.sin(data["WDIR"]/180*3.1415926)
                data["v"] = (-1)*data["WDSD"]*math.cos(data["WDIR"]/180*3.1415926)
                dataArr.append(data)
       
            field = "year,month,day,hour,date,station_id,ELEV,WDIR,WDSD,TEMP,HUMD,PRES,24R,u,v"
            keyStr = "year,month,day,hour,date,station_id,ELEV,WDIR,WDSD,TEMP,HUMD,PRES,24R,u,v"
            for data in dataArr:
                val = util.GenValue(data,keyStr)
                with self.connection.cursor() as cursor:
                    sql = "INSERT IGNORE INTO cwb_DATA ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                
            self.connection.commit()

    def CollectRain(self):
        print("Collect rain data")
        r = requests.get("https://opendata.cwb.gov.tw/fileapi/v1/opendataapi/O-A0002-001?format=JSON&Authorization="+self.key)
        #r.encoding = "utf-8"
        if r.status_code == requests.codes.all_okay:
            data = json.loads(r.text)
            for loc in data["cwbopendata"]["location"]:
                site = {}
                site["stationId"] = loc["stationId"]
                site["locationName"] = loc["locationName"]
                site["lat"] = float(loc["lat"])
                site["lon"] = float(loc["lon"])
                for param in loc["parameter"]:
                    name = param["parameterName"]
                    value = param["parameterValue"]
                    site[name] = value

                field = "stationId,locationName,lat,lon,CITY,CITY_SN,TOWN,TOWN_SN,ATTRIBUTE"
                val = util.GenValue(site,field)
                with self.connection.cursor() as cursor:
                    sql = "INSERT IGNORE INTO auto_rain_station ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)

                d = {}
                d["stationId"] = loc["stationId"]
                d["time"] = loc["time"]["obsTime"]
                for element in loc["weatherElement"]:
                    name = element["elementName"]
                    value = float(element["elementValue"]["value"])
                    d[name] = value
                field = "stationId,time,ELEV,RAIN,MIN_10,HOUR_3,HOUR_6,HOUR_12,HOUR_24,NOW,latest_2days,latest_3days"
                val = util.GenValue(d,field)
                with self.connection.cursor() as cursor:
                    sql = "INSERT IGNORE INTO auto_rain ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
            self.connection.commit()