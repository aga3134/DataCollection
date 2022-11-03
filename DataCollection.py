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
from BuoyData import BuoyData
from EPBData import EPBData
from MapLatLng import MapLatLng
import traceback

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
    buoy = BuoyData(connection,weatherKey)
    epb = EPBData(connection)
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
            buoy.CreateTable()
            epb.CreateTable()
            
        loopCollect = False
        if "loopCollect" in args:
            loopCollect = True
        
        if "epa" in args:
            try:
                #epa.CollectData()
                epa.CollectDataNCHU()
            except:
                print(sys.exc_info()[0])
                traceback.print_exc()
        if "power" in args:
            try:
                power.CollectData()
            except:
                print(sys.exc_info()[0])
                traceback.print_exc()
        if "weather" in args:
            try:
                weather.CollectData()
                weather.CollectDataNCHU()
            except:
                print(sys.exc_info()[0])
                traceback.print_exc()
        if "rain" in args:
            try:
                weather.CollectRain()
            except:
                print(sys.exc_info()[0])
                traceback.print_exc()
        if "traffic" in args:
            try:
                traffic.CollectData()
            except:
                print(sys.exc_info()[0])
                traceback.print_exc()
        if "cems6min" in args:
            try:
                cems.CollectData6min(loopCollect)
            except:
                print(sys.exc_info()[0])
                traceback.print_exc()
        if "cems15min" in args:
            try:
                cems.CollectData15min(loopCollect)
            except:
                print(sys.exc_info()[0])
                traceback.print_exc()
        if "cems1hour" in args:
            try:
                cems.CollectData1hour(loopCollect)
                cems.UpdateEmissionNCHU()
            except:
                print(sys.exc_info()[0])
                traceback.print_exc()
        if "addr" in args:
            try:
                mapLatLng.UpdateLocation()
            except:
                print(sys.exc_info()[0])
                traceback.print_exc()
        if "buoy" in args:
            try:
                buoy.CollectData()
            except:
                print(sys.exc_info()[0])
                traceback.print_exc()
        if "epb" in args:
            try:
                epb.CollectData()
            except:
                print(sys.exc_info()[0])
                traceback.print_exc()

        if "update" in args:
            try:
                epa.UpdateDataNCHU()
            except:
                print(sys.exc_info()[0])
                traceback.print_exc()
    
    
    connection.close()
