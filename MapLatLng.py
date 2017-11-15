# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 11:38:01 2017

@author: aga
"""

import json
import requests
import DataCollectUtil as util
import xml.etree.ElementTree as ET

class MapLatLng:
    def __init__(self, connection, apiKey):
        self.connection = connection
        self.key = apiKey
        
    def UpdateLocation(self):
        self.UpdatePowerLocation()
        self.UpdateCEMSLocation()
        
    def UpdatePowerLocation(self):
        print("Update Power Location")
        powerStation = json.loads(open("powerStation.json",encoding="utf8").read())
        
        with self.connection.cursor() as cursor:
            station = powerStation["station"]
            for s in station:
                name = s["name"]
                lat = s["lat"]
                lng = s["lng"]
                sql = "UPDATE PowerStations SET lat=%s, lng=%s WHERE name=%s"
                cursor.execute(sql,(lat,lng,name))
                
        self.connection.commit()
            
      
    def UpdateCEMSLocation(self):
        print("Update CEMS Location")
        
        with self.connection.cursor() as cursor:
            #update city code to name
            cityArr = {}
            cityArr["KHH"] = "高雄市"
            cityArr["TXG"] = "台中市"
            cityArr["ILA"] = "宜蘭縣"
            cityArr["CYQ"] = "嘉義縣"
            cityArr["TNN"] = "台南市"
            cityArr["YUN"] = "雲林縣"
            cityArr["CHA"] = "彰化縣"
            cityArr["TAO"] = "桃園市"
            cityArr["TPQ"] = "新北市"
            cityArr["TPE"] = "台北市"
            cityArr["HSQ"] = "新竹縣"
            for key in cityArr:
                sql = "UPDATE CEMSComps SET city='"+cityArr[key]+"' WHERE city='"+key+"'"
                cursor.execute(sql)
            
            #use google geocoding api to update empty addr,lat,lng
            sql = "SELECT name from CEMSComps WHERE addr IS NULL OR lat IS NULL or lng IS NULL"
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                name = row["name"]
                url = "https://maps.googleapis.com/maps/api/place/textsearch/json?language=zh-TW"
                url += "&query="+name
                url += "&key="+self.key
                r = requests.get(url)
                if r.status_code == requests.codes.all_okay:
                    data = r.json()
                    if(len(data["results"]) == 0):
                        continue
                    loc = data["results"][0]
                    addr = loc["formatted_address"]
                    lat = loc["geometry"]["location"]["lat"]
                    lng = loc["geometry"]["location"]["lng"]
                    sql = "UPDATE CEMSComps SET addr=%s, lat=%s, lng=%s WHERE name=%s"
                    cursor.execute(sql,(addr,lat,lng,name))
            
        self.connection.commit()
       