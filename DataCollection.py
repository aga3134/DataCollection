# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 15:48:59 2017

@author: aga
"""

import sys
import json
import pymysql
import warnings
from EPAData import EPAData
from PowerData import PowerData
from WeatherData import WeatherData
from TrafficData import TrafficData
from CEMSData import CEMSData

if __name__ == "__main__":
    config = json.loads(open("config.json").read())
    auth = config["mysqlAuth"]
    weatherKey = config["weatherKey"]
    connection = pymysql.connect(host=auth["host"],user=auth["username"],
            password=auth["password"],db=auth["dbName"],
            charset='utf8',cursorclass=pymysql.cursors.DictCursor)
    
    epa = EPAData(connection)
    power = PowerData(connection)
    weather = WeatherData(connection,weatherKey)
    traffic = TrafficData(connection)
    cems = CEMSData(connection)
    
    #ignore warning message
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', pymysql.Warning)
        args = sys.argv
        if "init" in args:
            epa.CreateTable()
            epa.AddSite()
            power.CreateTable()
            weather.CreateTable()
            traffic.CreateTable()
            traffic.AddSite()
            cems.CreateTable()
            cems.AddComp()
            cems.AddItem()
        
        if "epa" in args:
            epa.CollectData()
        if "power" in args:
            power.CollectData()
        if "weather" in args:
            weather.CollectData()
        if "traffic" in args:
            traffic.CollectData()
        if "cems" in args:
            cems.CollectData()
    
    #1小時1筆
    #epa = EPAData(connection)
    #epa.CreateTable()
    #epa.AddSite()
    #epa.CollectData()
    
    #10分鐘1筆
    #power = PowerData(connection)
    #power.CreateTable()
    #power.CollectData()
    
    #1小時1筆
    #weather = WeatherData(connection)
    #weather.CreateTable()
    #weather.CollectData()
    
    #5分鐘1筆
    #traffic = TrafficData(connection)
    #traffic.CreateTable()
    #traffic.AddSite()
    #traffic.CollectData()
    
    #一天取一次資料
    #cems = CEMSData(connection)
    #cems.CreateTable()
    #cems.AddComp()
    #cems.AddItem()
    #cems.CollectData()
    
    connection.close()