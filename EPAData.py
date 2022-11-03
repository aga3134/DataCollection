# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 11:38:01 2017

@author: aga
"""

import requests
import json
import DataCollectUtil as util
import datetime
import math
import re

class EPAData:
    def __init__(self, connection):
        self.connection = connection
        
    def CreateTable(self):
        print("Create Table for EPA Data")
        with self.connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS EPASites (\
                id VARCHAR(255),\
                engName VARCHAR(255),\
                areaName VARCHAR(255),\
                county VARCHAR(255),\
                township VARCHAR(255),\
                siteAddr VARCHAR(1024),\
                lat FLOAT,\
                lng FLOAT,\
                siteType VARCHAR(255),\
                PRIMARY KEY (id)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS EPAData (\
                siteName VARCHAR(255),\
                AQI FLOAT,\
                CO FLOAT,\
                NO FLOAT,\
                NO2 FLOAT,\
                NOx FLOAT,\
                O3 FLOAT,\
                PM10 FLOAT,\
                PM25 FLOAT,\
                SO2 FLOAT,\
                windDir INT,\
                windSpeed FLOAT,\
                time DATETIME,\
                PRIMARY KEY (siteName,time),\
                INDEX(siteName),\
                INDEX(time)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS epa_DATA (\
                year smallint(6),\
                month smallint(6),\
                day smallint(6),\
                hour smallint(6),\
                stationID VARCHAR(8),\
                jd smallint(6),\
                dateTime datetime,\
                SO2 double(12,8),\
                CO double(12,8),\
                O3 double(12,8),\
                PM10 double(12,6),\
                NOx double(12,8),\
                NO double(12,8),\
                NO2 double(12,8),\
                THC double(12,8),\
                NMHC double(14,10),\
                CH4 double(12,8),\
                wind double(12,8),\
                wDir double(12,6),\
                wDirStd double(10,6),\
                wDirGlobal double(10,6),\
                tmp double(10,6),\
                dpt double(10,2),\
                tmpIn double(10,6),\
                pres double(10,6),\
                pH double(10,6),\
                glibRad double(10,6),\
                ubvRad double(10,6),\
                netRad double(10,6),\
                pWatCond double(10,6),\
                pWat double(10,6),\
                pWatI double(10,6),\
                PMf double(10,6),\
                uGrd double(22,18),\
                vGrd double(22,18),\
                vapP double(10,6),\
                rh double(10,6),\
                uvi double(10,6),\
                uba double(10,6),\
                pWatHr double(10,6),\
                remark int(10),\
                tke double(8,6),\
                msrepl_tran_version varchar(38),\
                PMfCorr double(10,6),\
                PRIMARY KEY (year,month,day,hour,stationID),\
                INDEX(stationID),\
                INDEX(dateTime)\
                );"
            cursor.execute(sql)

        self.connection.commit()
        
    def AddSite(self):
        print("Add Sites for EPA Data")
        #r = requests.get("https://data.epa.gov.tw/api/v1/aqx_p_07?format=json&api_key=9be7b239-557b-4c10-9775-78cadfc555e9")
        r = requests.get("https://data.epa.gov.tw/api/v2/aqx_p_07?format=json&api_key=2295c87a-6124-41e2-b7f1-cc7728f17d6a")
        if r.status_code == requests.codes.all_okay:
            sites = r.json()["records"]
            field = "id,engName,areaName,county,township,siteAddr,lng,lat,siteType"
            keyStr = "SiteName,SiteEngName,AreaName,County,Township,SiteAddress,TWD97Lon,TWD97Lat,SiteType"
            
            for site in sites:
                val = util.GenValue(site,keyStr)
                with self.connection.cursor() as cursor:
                    sql = "INSERT IGNORE INTO EPASites ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                    
            self.connection.commit()
        
    def CollectData(self):
        print("Collect EPA Data")
        r = requests.get("http://opendata2.epa.gov.tw/AQI.json")
        if r.status_code == requests.codes.all_okay:
            sites = r.json()
            field = "siteName,AQI,CO,NO,NO2,NOx,O3,PM10,PM25,SO2,windDir,windSpeed,time"
            keyStr = "SiteName,AQI,CO,NO,NO2,NOx,O3,PM10,PM2.5,SO2,WIND_DIREC,WIND_SPEED,PublishTime"
            
            for site in sites:
                val = util.GenValue(site,keyStr)
                with self.connection.cursor() as cursor:
                    sql = "INSERT IGNORE INTO EPAData ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                    
            self.connection.commit()
        
    def CollectDataNCHU(self):
        print("Collect EPA Data NCHU")
        #update air quality data
        #r = requests.get("https://data.epa.gov.tw/api/v1/aqx_p_432?format=json5&api_key=9be7b239-557b-4c10-9775-78cadfc555e9")
        r = requests.get("https://data.epa.gov.tw/api/v2/aqx_p_432?format=json&api_key=2295c87a-6124-41e2-b7f1-cc7728f17d6a")
        if r.status_code == requests.codes.all_okay:
            records = r.json()["records"]
            field = "year,month,day,hour,stationID,dateTime,SO2,CO,O3,PM10,NOx,NO,NO2,wind,wDir,uGrd,vGrd,PMfCorr,PMf"
            keyStr = "year,month,day,hour,stationID,dateTime,SO2,CO,O3,PM10,NOx,NO,NO2,wind,wDir,uGrd,vGrd,PMfCorr,PMf"
            stationField = "StationID,StationName,lat,lon"
            
            for record in records:
                data = {}
                siteName = record["sitename"]
                
                data["stationID"] = "EPA"+util.PadLeft(record["siteid"],"0",3)
                dateStr = record["publishtime"]
                dateObj = datetime.datetime.strptime(dateStr, "%Y/%m/%d %H:%M:%S")
                #將發佈時間減一小時對齊資料實際時間
                dateObj = dateObj - datetime.timedelta(hours=1)
                data["dateTime"] = dateObj.strftime('%Y-%m-%d %H:%M:%S')
                data["year"] = dateObj.year
                data["month"] = dateObj.month
                data["day"] = dateObj.day
                data["hour"] = dateObj.hour
                
                data["SO2"] = util.ToFloat(record["so2"])
                data["CO"] = util.ToFloat(record["co"])
                data["O3"] = util.ToFloat(record["o3"])
                data["PM10"] = util.ToFloat(record["pm10"])
                data["NOx"] = util.ToFloat(record["nox"])
                data["NO"] = util.ToFloat(record["no"])
                data["NO2"] = util.ToFloat(record["no2"])
                data["wind"] = util.ToFloat(record["wind_speed"])
                data["wDir"] = util.ToFloat(record["wind_direc"])
                if data["wind"] is None or data["wDir"] is None:
                    data["uGrd"] = None
                    data["vGrd"] = None
                else:
                    data["uGrd"] = (-1)*data["wind"]*math.sin(data["wDir"]/180*3.1415926)
                    data["vGrd"] = (-1)*data["wind"]*math.cos(data["wDir"]/180*3.1415926)
                data["PMfCorr"] = util.ToFloat(record["pm2.5"])
                data["PMf"] = util.ToFloat(record["pm2.5"])
                
                val = util.GenValue(data,keyStr)
                with self.connection.cursor() as cursor:
                    sql = "INSERT IGNORE INTO epa_DATA ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)

                #add station if not exist
                station = {}
                station["StationID"] = data["stationID"]
                station["StationName"] = siteName
                station["lat"] = util.ToFloat(record["latitude"])
                station["lon"] = util.ToFloat(record["longitude"])
                val = util.GenValue(station,stationField)
                with self.connection.cursor() as cursor:
                    sql = "INSERT IGNORE INTO epa_stationID ("+stationField+") VALUES ("+val+")"
                    cursor.execute(sql)
                    
            self.connection.commit()
            
        #update weather quality data
        #r = requests.get("https://data.epa.gov.tw/api/v1/aqx_p_35?format=json&limit=1000&api_key=f9d148ba-6ffb-45a2-a227-ebf0208a4ef8")
        r = requests.get("https://data.epa.gov.tw/api/v2/aqx_p_35?format=json&limit=1000&api_key=2295c87a-6124-41e2-b7f1-cc7728f17d6a")
        if r.status_code == requests.codes.all_okay:
            records = r.json()["records"]
            for record in records:
                siteName = record["sitename"]
                sID  = "EPA"+util.PadLeft(record["siteid"],"0",3)

                dateStr = record["monitordate"]
                dateObj = datetime.datetime.strptime(dateStr, "%Y-%m-%d %H:%M")
                #將發佈時間減一小時對齊資料實際時間
                dateObj = dateObj - datetime.timedelta(hours=1)
                q = "year="+str(dateObj.year)
                q += " AND month="+str(dateObj.month)
                q += " AND day="+str(dateObj.day)
                q += " AND hour="+str(dateObj.hour)
                q += " AND stationID='"+sID+"'"
                
                if record["itemname"] == "溫度":
                    with self.connection.cursor() as cursor:
                        q += "AND tmp IS NULL"
                        temp = util.ToFloat(record["concentration"])
                        if temp is not None:
                            temp = temp+273.15
                            sql = "UPDATE epa_DATA SET tmp="+str(temp)+" WHERE "+q
                            cursor.execute(sql)
                    
                if record["itemname"] == "相對濕度":
                    with self.connection.cursor() as cursor:
                        q += "AND rh IS NULL"
                        rh = util.ToFloat(record["concentration"])
                        if rh is not None:
                            sql = "UPDATE epa_DATA SET rh="+str(rh)+" WHERE "+q
                            cursor.execute(sql)
                        
            self.connection.commit()

    def UpdateDataNCHU(self):
        print("Update EPA Data NCHU")
        offset = 0
        limit = 1000
        maxOffset = 2000
        while offset < maxOffset:
            url = "https://data.epa.gov.tw/api/v2/aqx_p_488?format=json&api_key=2295c87a-6124-41e2-b7f1-cc7728f17d6a&offset=%d&limit=%d" % (offset,limit)
            print(url)
            r = requests.get(url)

            if r.status_code == requests.codes.all_okay:
                records = r.json()["records"]
                field = "year,month,day,hour,stationID,dateTime,SO2,CO,O3,PM10,NOx,NO,NO2,wind,wDir,uGrd,vGrd,PMfCorr,PMf"
                keyStr = "year,month,day,hour,stationID,dateTime,SO2,CO,O3,PM10,NOx,NO,NO2,wind,wDir,uGrd,vGrd,PMfCorr,PMf"

                for record in records:
                    data = {}
                    data["stationID"] = "EPA"+util.PadLeft(record["siteid"],"0",3)
                    dateStr = record["datacreationdate"]
                    dateObj = datetime.datetime.strptime(dateStr, "%Y-%m-%d %H:%M")
                    #將發佈時間減一小時對齊資料實際時間
                    dateObj = dateObj - datetime.timedelta(hours=1)
                    data["dateTime"] = dateObj.strftime('%Y-%m-%d %H:%M:%S')
                    data["year"] = dateObj.year
                    data["month"] = dateObj.month
                    data["day"] = dateObj.day
                    data["hour"] = dateObj.hour
                    
                    data["SO2"] = util.ToFloat(record["so2"])
                    data["CO"] = util.ToFloat(record["co"])
                    data["O3"] = util.ToFloat(record["o3"])
                    data["PM10"] = util.ToFloat(record["pm10"])
                    data["NOx"] = util.ToFloat(record["nox"])
                    data["NO"] = util.ToFloat(record["no"])
                    data["NO2"] = util.ToFloat(record["no2"])
                    data["wind"] = util.ToFloat(record["windspeed"])
                    data["wDir"] = util.ToFloat(record["winddirec"])
                    if data["wind"] is None or data["wDir"] is None:
                        data["uGrd"] = None
                        data["vGrd"] = None
                    else:
                        data["uGrd"] = (-1)*data["wind"]*math.sin(data["wDir"]/180*3.1415926)
                        data["vGrd"] = (-1)*data["wind"]*math.cos(data["wDir"]/180*3.1415926)
                    data["PMfCorr"] = util.ToFloat(record["pm2.5"])
                    data["PMf"] = util.ToFloat(record["pm2.5"])
                    
                    val = util.GenValue(data,keyStr)
                    with self.connection.cursor() as cursor:
                        sql = "INSERT INTO epa_DATA ("+field+") VALUES ("+val+") ON DUPLICATE KEY UPDATE "
                        arr = []
                        for key in ["SO2","CO","O3","PM10","NOx","NO","NO2","wind","wDir","uGrd","vGrd","PMfCorr","PMf"]:
                            if data[key] is not None:
                                arr.append("%s=VALUES(%s)" % (key,key))
                        if len(arr) == 0:
                            continue
                        sql += ",".join(arr)
                        #print(sql)
                        cursor.execute(sql)
                        
                self.connection.commit()

            offset += limit
