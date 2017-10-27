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
                amount INT,\
                time DATETIME,\
                PRIMARY KEY (id)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS CEMSData (\
                c_no VARCHAR(255),\
                p_no VARCHAR(255),\
                item VARCHAR(255),\
                value FLOAT,\
                time DATETIME,\
                PRIMARY KEY (c_no,p_no,item,time),\
                INDEX(c_no),\
                INDEX(item),\
                INDEX(time),\
                );"
            cursor.execute(sql)

        self.connection.commit()
        
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
        
    def CollectData(self):
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
            
        
        