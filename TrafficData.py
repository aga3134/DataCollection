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

class TrafficData:
    def __init__(self, connection):
        self.connection = connection
        
    def CreateTable(self):
        print("Create Table for Traffic Data")
        with self.connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS TrafficSites (\
                id VARCHAR(255),\
                lat FLOAT,\
                lng FLOAT,\
                highway VARCHAR(255),\
                interchange VARCHAR(255),\
                PRIMARY KEY (id)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS TrafficFlow (\
                siteID VARCHAR(255),\
                dir VARCHAR(1),\
                type VARCHAR(255),\
                amount INT,\
                time DATETIME,\
                PRIMARY KEY (siteID,type,time),\
                INDEX(siteID),\
                INDEX(type),\
                INDEX(time)\
                );"
            cursor.execute(sql)

        self.connection.commit()
        
    def AddSite(self):
        print("Add Traffic Sites")
        r = requests.get("http://www.freeway.gov.tw/Upload/DownloadFiles/國道計費門架座標及里程牌價表.csv")
        if r.status_code == requests.codes.all_okay:
            field = "id,lat,lng,highway,interchange"
            keyStr = "id,lat,lng,highway,interchange"
            with self.connection.cursor() as cursor:
                rows = r.text.split("\n")
                for row in rows:
                    column = row.split(",")
                    if len(column) < 9:
                        continue
                    site = {}
                    siteID = column[2].replace("-","").replace(".","")
                    site["id"] = siteID
                    site["lat"] = column[7]
                    site["lng"] = column[8]
                    site["highway"] = column[0]
                    site["interchange"] = column[5]+" - "+column[6]
                    val = util.GenValue(site,keyStr)
                
                    sql = "INSERT IGNORE INTO TrafficSites ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                
            self.connection.commit()
    
    def TypeToName(self,t):
        name = ""
        if t == "31":
            name = "小客車"
        elif t == "32":
            name = "小貨車"
        elif t == "41":
            name = "大客車"
        elif t == "42":
            name = "大貨車"
        elif t == "5":
            name = "聯結車"
        return name
    
    def CollectTrafficFlow(self):
        print("Collect Traffic Flow")
        #取20分鐘前的統計資料
        now = datetime.datetime.now()
        min20Before = now + datetime.timedelta(minutes=-20)
        tw = pytz.timezone('Asia/Taipei')
        min20Before.astimezone(tw)
        date = datetime.datetime.strftime(min20Before, '%Y%m%d')
        hour = datetime.datetime.strftime(min20Before, '%H')
        minute = int(datetime.datetime.strftime(min20Before, '%M'))
        #五分鐘一筆資料
        minute = str(minute - minute%5)
        if len(minute) == 1:
            minute = "0"+minute
        
        path = "http://tisvcloud.freeway.gov.tw/history/TDCS/M03A/"+date+"/"+hour+"/"
        file = "TDCS_M03A_"+date+"_"+hour+minute+"00.csv"
        r = requests.get(path+file)
        #r.encoding = "utf-8"
        if r.status_code == requests.codes.all_okay:
            field = "siteID,time,dir,type,amount"
            keyStr = "siteID,time,dir,type,amount"

            with self.connection.cursor() as cursor:
                rows = r.text.split("\n")
                for row in rows:
                    column = row.split(",")
                    if len(column) < 5:
                        continue
                    data = {}
                    data["siteID"] = column[1]
                    data["time"] = column[0]
                    data["dir"] = column[2]
                    data["type"] = self.TypeToName(column[3])
                    data["amount"] = column[4]
                    val = util.GenValue(data,keyStr)
                
                    sql = "INSERT IGNORE INTO TrafficFlow ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                
            self.connection.commit()
        
    def CollectData(self):
        self.CollectTrafficFlow()
            
        
        