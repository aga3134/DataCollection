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

class CEMSData:
    def __init__(self, connection):
        self.connection = connection
        
    def CreateTable(self):
        print("Create Table for CEMS Data")
        with self.connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS CEMSComps (\
                id VARCHAR(255),\
                name VARCHAR(255),\
                addr VARCHAR(255),\
                lat FLOAT,\
                lng FLOAT,\
                city VARCHAR(255),\
                PRIMARY KEY (id)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS CEMSItems (\
                id VARCHAR(255),\
                name VARCHAR(255),\
                desp VARCHAR(255),\
                unit VARCHAR(255),\
                PRIMARY KEY (id)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS CEMSData (\
                c_no VARCHAR(255),\
                p_no VARCHAR(255),\
                item VARCHAR(255),\
                value FLOAT,\
                time DATETIME,\
                statusCode VARCHAR(5),\
                PRIMARY KEY (c_no,p_no,item,time),\
                INDEX(c_no),\
                INDEX(item),\
                INDEX(time)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS CEMSStd (\
                c_no VARCHAR(255),\
                p_no VARCHAR(255),\
                item VARCHAR(255),\
                std VARCHAR(255),\
                desp VARCHAR(255),\
                PRIMARY KEY (c_no,p_no,item),\
                INDEX(c_no),\
                INDEX(item)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS CEMSStatus (\
                statusCode VARCHAR(5),\
                desp VARCHAR(255),\
                PRIMARY KEY (statusCode)\
                );"
            cursor.execute(sql)

        self.connection.commit()
        
    def ExtractCEMSData(self,data):
        d = {}
        d["c_no"] = data["CNO"].strip()
        d["p_no"] = data["PolNo"].strip()
        d["item"] = data["Item"].strip()
        d["value"] = data["M_Val"]
        d["time"] = data["M_Time"]
        d["statusCode"] = data["Code2"].strip()
        return d
    
    def ExtractCEMSComp(self,data):
        comp = {}
        comp["id"] = data["CNO"].strip()
        comp["name"] = data["Abbr"]
        comp["city"] = data["Epb"].strip()
        #lat,lng,addr用google geocoding api更新
        return comp
    
    def ExtractCEMSItem(self,data):
        item = {}
        item["id"] = data["Item"].strip()
        item["desp"] = data["ItemDesc"]
        item["unit"] = data["Unit"].strip()
        #name手動設定
        return item
    
    def ExtractCEMSStd(self,data):
        std = {}
        std["c_no"] = data["CNO"].strip()
        std["p_no"] = data["PolNo"].strip()
        std["item"] = data["Item"].strip()
        std["std"] = data["Std"]
        std["desp"] = data["Std_s"]
        return std    
    
    def ExtractCEMSStatus(self,data):
        status = {}
        status["statusCode"] = data["Code2"].strip()
        status["desp"] = data["Code2Desc"]
        return status
    
    def CollectDataFromUrl(self, url, loopCollect):
        skip = 0
        fetchNum = 1000
        dataExist = False
        loop = True
        while(loop and not dataExist):
            dataUrl = url
            dataUrl += "?$format=json&$orderby=M_Time%20desc&$skip="+str(skip)+"&$top="+str(fetchNum)
            #print(dataUrl)
            r = requests.get(dataUrl)
            if r.status_code != requests.codes.all_okay:
                return
            
            data = r.json()
            with self.connection.cursor() as cursor:
                dataArr = {}
                compArr = {}
                itemArr = {}
                stdArr = {}
                statusArr = {}
                for d in data:
                    dataID = d["CNO"]+d["PolNo"]+d["Item"]+d["M_Time"]
                    compID = d["CNO"]
                    itemID = d["Item"]
                    stdID = d["CNO"]+d["PolNo"]+d["Item"]
                    statusID = d["Code2"]
                    dataArr[dataID] = self.ExtractCEMSData(d)
                    compArr[compID] = self.ExtractCEMSComp(d)
                    itemArr[itemID] = self.ExtractCEMSItem(d)
                    stdArr[stdID] = self.ExtractCEMSStd(d)
                    statusArr[statusID] = self.ExtractCEMSStatus(d)
                    
                #check最後一筆資料是否己存在資料庫，若己存在就不用再往下展開
                lastData = self.ExtractCEMSData(data[len(data)-1])
                sql = "SELECT * FROM CEMSData WHERE c_no='"+lastData["c_no"]+"'"
                sql += "AND p_no='"+lastData["p_no"]+"'"
                sql += "AND item='"+lastData["item"]+"'"
                sql += "AND time='"+lastData["time"]+"'"
                cursor.execute(sql)
                row = cursor.fetchone()
                if row:
                    dataExist = True
                    
                #加入資料到資料庫
                field = "c_no,p_no,item,value,time,statusCode"
                keyStr = "c_no,p_no,item,value,time,statusCode"
                for key in dataArr:
                    val = util.GenValue(dataArr[key],keyStr)
                    sql = "INSERT IGNORE INTO CEMSData ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                
                field = "id,name,city"
                keyStr = "id,name,city"
                for key in compArr:
                    val = util.GenValue(compArr[key],keyStr)
                    sql = "INSERT IGNORE INTO CEMSComps ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                    
                field = "id,desp,unit"
                keyStr = "id,desp,unit"
                for key in itemArr:
                    val = util.GenValue(itemArr[key],keyStr)
                    sql = "INSERT IGNORE INTO CEMSItems ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                    
                field = "c_no,p_no,item,std,desp"
                keyStr = "c_no,p_no,item,std,desp"
                for key in stdArr:
                    val = util.GenValue(stdArr[key],keyStr)
                    sql = "INSERT IGNORE INTO CEMSStd ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                    
                field = "statusCode,desp"
                keyStr = "statusCode,desp"
                for key in statusArr:
                    val = util.GenValue(statusArr[key],keyStr)
                    sql = "INSERT IGNORE INTO CEMSStatus ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                
            self.connection.commit()
            
            #展開下fetchNum筆資料
            skip += fetchNum
            loop = loopCollect
                
        
    def CollectData6min(self, loopCollect):
        print("Collect CEMS Data 6min")
        self.CollectDataFromUrl("http://opendata.epa.gov.tw/ws/Data/POP00048/",loopCollect);
        
    def CollectData15min(self, loopCollect):
        print("Collect CEMS Data 15min")
        self.CollectDataFromUrl("http://opendata.epa.gov.tw/ws/Data/POP00049/",loopCollect);
        
    def CollectData1hour(self, loopCollect):
        print("Collect CEMS Data 1hour")
        self.CollectDataFromUrl("http://opendata.epa.gov.tw/ws/Data/POP00053/",loopCollect);
        
            
    def AddComp(self):
        print("Add Company for CEMS Data")
        r = requests.get("http://ks-opendata-community.github.io/chimney/data/工廠清單.json")
        if r.status_code == requests.codes.all_okay:
            field = "id,name,addr,lat,lng,city"
            keyStr = "管制編號,工廠,地址,Lat,Lng,city"
            with self.connection.cursor() as cursor:
                comps = r.json()
                for comp in comps:
                    val = util.GenValue(comp,keyStr)
                    sql = "INSERT IGNORE INTO CEMSComps ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                
            self.connection.commit()
            
    def AddItem(self):
        print("Add Item for CEMS Data")
        r = requests.get("http://ks-opendata-community.github.io/chimney/data/項目代碼.json")
        if r.status_code == requests.codes.all_okay:
            field = "id,name,desp,unit"
            keyStr = "ITEM,ABBR,DESP,UNIT"
            with self.connection.cursor() as cursor:
                items = r.json()
                for item in items:
                    val = util.GenValue(item,keyStr)
                    sql = "INSERT IGNORE INTO CEMSItems ("+field+") VALUES ("+val+")"
                    cursor.execute(sql)
                
            self.connection.commit()
        
    """def CollectData(self):
        #取前一天的統計資料
        now = datetime.datetime.now()
        yestoday = now + datetime.timedelta(days=-1)
        tw = pytz.timezone('Asia/Taipei')
        yestoday.astimezone(tw)
        year = datetime.datetime.strftime(yestoday, '%Y')
        month = datetime.datetime.strftime(yestoday, '%m')
        day = datetime.datetime.strftime(yestoday, '%d')
        date = datetime.datetime.strftime(yestoday, '%Y%m%d')
        
        #目前有cems的城市
        #高雄 kaohsiung KHH
        #台中 taichung TXG
        #宜蘭 yilan ILA
        #嘉義 chiayi CYQ
        #台南 tainan TNN
        #雲林 yunlin YUN
        #彰化 changhua CHA
        #桃園 taoyuan TAO
        #新北 newtaipei TPQ
        #台北 taipei TPE
        #新竹 hsinchu HSQ
        cities = ["kaohsiung","taichung","yilan","chiayi","tainan","yunlin","changhua","taoyuan","newtaipei","taipei","hsinchu"]
        
        for city in cities:
            print("Collect CEMS Data for "+city)
            path = "http://ks-opendata-community.github.io/chimney/data/daily/"
            file = city+"/"+year+"/"+month+"/"+date+".csv"
            r = requests.get(path+file)
            #r.encoding = "utf-8"
            if r.status_code == requests.codes.all_okay:
                field = "c_no,p_no,item,value,time"
                keyStr = "c_no,p_no,item,value,time"
    
                with self.connection.cursor() as cursor:
                    rows = r.text.split("\n")
                    for row in rows:
                        column = row.split(",")
                        if len(column) < 5:
                            continue
                        data = {}
                        data["c_no"] = column[0]
                        data["p_no"] = column[1]
                        data["item"] = column[2]
                        t = year+"/"+month+"/"+day+" "+column[3][0:2]+":"+column[3][2:4]+":00"
                        data["time"] = t
                        data["value"] = column[4]
                        val = util.GenValue(data,keyStr)
                    
                        sql = "INSERT IGNORE INTO CEMSData ("+field+") VALUES ("+val+")"
                        cursor.execute(sql)
                    
                self.connection.commit()
            """
        
        