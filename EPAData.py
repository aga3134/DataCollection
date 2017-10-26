# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 11:38:01 2017

@author: aga
"""

import requests
import json
import DataCollectUtil as util

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
                INDEX(time),\
                FOREIGN KEY (siteName) REFERENCES EPASites(id)\
                );"
            cursor.execute(sql)

        self.connection.commit()
        
    def AddSite(self):
        print("Add Sites for EPA Data")
        r = requests.get("http://opendata.epa.gov.tw/ws/Data/AQXSite/?$format=json")
        if r.status_code == requests.codes.all_okay:
            sites = r.json()
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
            keyStr = "SiteName,AQI,CO,NO,NO2,NOx,O3,PM10,PM2.5,SO2,WindDirec,WindSpeed,PublishTime"
            
            for site in sites:
                val = util.GenValue(site,keyStr)
                with self.connection.cursor() as cursor:
                    sql = "INSERT IGNORE INTO EPAData ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                    
            self.connection.commit()
        
        