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
import pymysql

class PowerData:
    def __init__(self, connection):
        self.connection = connection
        
    def CreateTable(self):
        print("Create Table for Power Data")
        with self.connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS PowerStations (\
                id VARCHAR(32),\
                name VARCHAR(255),\
                lat FLOAT,\
                lng FLOAT,\
                capacity FLOAT,\
                type VARCHAR(255),\
                PRIMARY KEY (id)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS PowerGens (\
                powerGen_id VARCHAR(32)\
                stationID VARCHAR(10),\
                powerGen FLOAT,\
                remark VARCHAR(255),\
                time DATETIME,\
                PRIMARY KEY (stationID,time),\
                INDEX(stationID),\
                INDEX(time)\
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
       
    def ToFloat(self,s):
        try:
            if isinstance(s, float) or isinstance(s, int):
                return float(s)
            else:
                return float(s.replace(",",""))
        except ValueError:
            return None
 
    def CollectPowerGen(self):
        print("Collect Power Generation")
        
        #load station id mapping table
        config = json.loads(open("config.json").read())
        auth = config["mysqlAuth"]
        powerConn = pymysql.connect(host=auth["host"],user=auth["username"],
            password=auth["password"],db="PowerGen",
            charset='utf8',cursorclass=pymysql.cursors.DictCursor)
        idMapping = {}
        with powerConn.cursor() as cursor:
            sql = "SELECT powerGen_id,stationID from station_power"
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                idMapping[row["powerGen_id"]] = row["stationID"]
        #print(idMapping)

        r = requests.get("https://data.taipower.com.tw/opendata01/apply/file/d006001/001.txt")
        r.encoding = "utf-8"
        if r.status_code == requests.codes.all_okay:
            data = json.loads(r.text)
            t = data[""]
            stationArr = []
            dataArr = []
            typeMapping = {
                "核能":"nuclear","燃煤":"coal","汽電共生":"cogen","民營電廠-燃煤":"ippcoal","燃氣":"lng","民營電廠-燃氣":"ipplng","燃油":"oil","輕油":"diesel","水力":"hydro","風力":"wind","太陽能":"solar","抽蓄發電":"pumpinggen","抽蓄負載":"pumpingload"
            }
            for device in data["aaData"]:
                if device[1] == "小計":
                    continue
                if device[0] in typeMapping:
                    device[0] = typeMapping[device[0]]
                station = {}
                station["type"] = device[0]
                station["name"] = device[1]
                station["id"] = station["type"]+"_"+station["name"]
                station["capacity"] = device[2]
                stationArr.append(station)
                
                data = {}
                data["powerGen_id"] = station["id"]
                data["stationID"] = None
                if station["id"] in idMapping:
                    data["stationID"] = idMapping[station["id"]]
                data["powerGen"] = self.ToFloat(device[3])
                data["device_capacity"] = self.ToFloat(device[2])
                data["capacity_factor"] = None
                if data["powerGen"] is not None and data["device_capacity"] is not None:
                    data["capacity_factor"] = data["powerGen"]/data["device_capacity"]
                data["remark"] = device[5]
                data["time"] = t
                dataArr.append(data)
                #print(data)
                
            field = "id,name,capacity,type"
            keyStr = "id,name,capacity,type"
            for station in stationArr:
                val = util.GenValue(station,keyStr)
                with self.connection.cursor() as cursor:
                    sql = "INSERT IGNORE INTO PowerStations ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                    
            field = "powerGen_id,stationID,powerGen,device_capacity,capacity_factor,remark,time"
            keyStr = "powerGen_id,stationID,powerGen,device_capacity,capacity_factor,remark,time"
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
        
        r = requests.get("https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/loadareas.csv")
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
        
        r = requests.get("https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/loadfueltype.csv")
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
        
        
