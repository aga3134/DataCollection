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
from MapLatLng import MapLatLng

if __name__ == "__main__":
    config = json.loads(open("config.json").read())
    auth = config["mysqlAuth"]
    weatherKey = config["weatherKey"]
    googleMapKey = config["googleMapKey"]
    connection = pymysql.connect(host=auth["host"],user=auth["username"],
            password=auth["password"],db=auth["dbName"],
            charset='utf8',cursorclass=pymysql.cursors.DictCursor)
    
    epa = EPAData(connection)
    power = PowerData(connection)
    weather = WeatherData(connection,weatherKey)
    traffic = TrafficData(connection)
    cems = CEMSData(connection)
    mapLatLng = MapLatLng(connection,googleMapKey)
    
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
            
        loopCollect = False
        if "loopCollect" in args:
            loopCollect = True
        
        if "epa" in args:
            try:
                epa.CollectData()
                epa.CollectDataNCHU()
            except:
                print(sys.exc_info()[0])
        if "power" in args:
            try:
                power.CollectData()
            except:
                print(sys.exc_info()[0])
        if "weather" in args:
            try:
                weather.CollectData()
                weather.CollectDataNCHU()
            except:
                print(sys.exc_info()[0])
        if "traffic" in args:
            try:
                traffic.CollectData()
            except:
                print(sys.exc_info()[0])
        if "cems6min" in args:
            try:
                cems.CollectData6min(loopCollect)
            except:
                print(sys.exc_info()[0])
        if "cems15min" in args:
            try:
                cems.CollectData15min(loopCollect)
            except:
                print(sys.exc_info()[0])
        if "cems1hour" in args:
            try:
                cems.CollectData1hour(loopCollect)
                #cems.UpdateEmissionNCHU()
            except:
                print(sys.exc_info()[0])
        if "addr" in args:
            try:
                mapLatLng.UpdateLocation()
            except:
                print(sys.exc_info()[0])
    
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