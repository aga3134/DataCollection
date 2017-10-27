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

class PowerData:
    def __init__(self, connection):
        self.connection = connection
        
    def CreateTable(self):
        print("Create Table for Power Data")
        with self.connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS PowerStations (\
                id VARCHAR(255),\
                name VARCHAR(255),\
                lat FLOAT,\
                lng FLOAT,\
                capacity FLOAT,\
                type VARCHAR(255),\
                PRIMARY KEY (id)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS PowerGens (\
                stationID VARCHAR(255),\
                powerGen FLOAT,\
                remark VARCHAR(255),\
                time DATETIME,\
                PRIMARY KEY (stationID,time),\
                INDEX(stationID),\
                INDEX(time),\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS PowerLoads (\
                time DATETIME,\
                north FLOAT,\
                central FLOAT,\
                south FLOAT,\
                east FLOAT,\
                PRIMARY KEY (time)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS PowerRatios (\
                time DATETIME,\
                nuclear FLOAT,\
                coal FLOAT,\
                coGen FLOAT,\
                ippCoal FLOAT,\
                lng FLOAT,\
                ippLng FLOAT,\
                oil FLOAT,\
                diesel FLOAT,\
                hydro FLOAT,\
                wind FLOAT,\
                solar FLOAT,\
                pumpGen FLOAT,\
                pumpLoad FLOAT,\
                PRIMARY KEY (time)\
                );"
            cursor.execute(sql)

        self.connection.commit()
        
    def CollectPowerGen(self):
        print("Collect Power Generation")
        r = requests.get("http://www.taipower.com.tw/loadGraph/loadGraph/data/genary.txt")
        r.encoding = "utf-8"
        if r.status_code == requests.codes.all_okay:
            txt = "{\"time"+r.text[2:]
            
            data = json.loads(txt)
            t = data["time"]
            stationArr = []
            dataArr = []
            for device in data["aaData"]:
                if device[1] == "小計":
                    continue
                station = {}
                station["type"] = device[0].split("'")[1]
                station["name"] = device[1]
                station["id"] = station["type"]+"_"+station["name"]
                station["capacity"] = device[2]
                stationArr.append(station)
                
                data = {}
                data["stationID"] = station["id"]
                data["powerGen"] = device[3]
                data["remark"] = device[5]
                data["time"] = t
                dataArr.append(data)
                
            field = "id,name,capacity,type"
            keyStr = "id,name,capacity,type"
            for station in stationArr:
                val = util.GenValue(station,keyStr)
                with self.connection.cursor() as cursor:
                    sql = "INSERT IGNORE INTO PowerStations ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                    
            field = "stationID,powerGen,remark,time"
            keyStr = "stationID,powerGen,remark,time"
            for data in dataArr:
                val = util.GenValue(data,keyStr)
                with self.connection.cursor() as cursor:
                    sql = "INSERT IGNORE INTO PowerGens ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                
            self.connection.commit()
            
    def Get10minBefore(self):
        now = datetime.datetime.now()
        min10Before = now + datetime.timedelta(minutes=-10)
        tw = pytz.timezone('Asia/Taipei')
        min10Before.astimezone(tw)
        return min10Before
            
    def CollectPowerLoad(self):
        print("Collect Power Load")
        #loadareas資料在凌晨12點取時會得到前一天的完整資料，往前調10分鐘確保取到正確日期
        date = datetime.datetime.strftime(self.Get10minBefore(), '%Y-%m-%d')
        
        r = requests.get("http://www.taipower.com.tw/loadGraph/loadGraph/data/loadareas.csv")
        if r.status_code == requests.codes.all_okay:
            arr = r.text.split("\n")
            field = "time,east,south,central,north"
            keyStr = "time,east,south,central,north"
            
            with self.connection.cursor() as cursor:
                for record in arr:
                    data = record.split(",")
                    if len(data) < 5:
                        continue
                    load = {}
                    load["time"] = date+" "+data[0]
                    load["east"] = data[1]
                    load["south"] = data[2]
                    load["central"] = data[3]
                    load["north"] = data[4]
                    val = util.GenValue(load,keyStr)
                
                    sql = "INSERT IGNORE INTO PowerLoads ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
            
            self.connection.commit()
    
    def CollectPowerRatio(self):
        print("Collect Power Ratio")
        #loadareas資料在凌晨12點取時會得到前一天的完整資料，往前調10分鐘確保取到正確日期
        date = datetime.datetime.strftime(self.Get10minBefore(), '%Y-%m-%d')
        
        r = requests.get("http://stpc00601.taipower.com.tw/loadGraph/loadGraph/data/loadfueltype.csv")
        if r.status_code == requests.codes.all_okay:
            arr = r.text.split("\n")
            field = "time,nuclear,coal,coGen,ippCoal,lng,ippLng,oil,diesel,hydro,wind,solar,pumpGen,pumpLoad"
            keyStr = "time,nuclear,coal,coGen,ippCoal,lng,ippLng,oil,diesel,hydro,wind,solar,pumpGen,pumpLoad"
            
            with self.connection.cursor() as cursor:
                for record in arr:
                    data = record.split(",")
                    if len(data) < 14:
                        continue
                    ratio = {}
                    ratio["time"] = date+" "+data[0]
                    ratio["nuclear"] = data[1]
                    ratio["coal"] = data[2]
                    ratio["coGen"] = data[3]
                    ratio["ippCoal"] = data[4]
                    ratio["lng"] = data[5]
                    ratio["ippLng"] = data[6]
                    ratio["oil"] = data[7]
                    ratio["diesel"] = data[8]
                    ratio["hydro"] = data[9]
                    ratio["wind"] = data[10]
                    ratio["solar"] = data[11]
                    ratio["pumpGen"] = data[12]
                    ratio["pumpLoad"] = data[13]
                    val = util.GenValue(ratio,keyStr)
                
                    sql = "INSERT IGNORE INTO PowerRatios ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
            
            self.connection.commit()
        
    def CollectData(self):
        self.CollectPowerGen()
        self.CollectPowerLoad()
        self.CollectPowerRatio()
        
        