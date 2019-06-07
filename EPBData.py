# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 11:02:14 2019

@author: aga
"""

import requests
import json
import datetime
import pytz
import DataCollectUtil as util
import xml.etree.ElementTree as ET
import math

class EPBData:
    def __init__(self, connection):
        self.connection = connection
        
    def CreateTable(self):
        print("Create Table for EPBData")
        with self.connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS EPBData (\
                SiteId VARCHAR(255),\
                SiteName VARCHAR(255),\
                County VARCHAR(255),\
                ItemId VARCHAR(255),\
                ItemName VARCHAR(255),\
                ItemEngName VARCHAR(255),\
                ItemUnit VARCHAR(255),\
                MonitorDate DATETIME,\
                Concentration FLOAT,\
                PRIMARY KEY (SiteId,MonitorDate,ItemEngName),\
                INDEX(MonitorDate)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS epb_DATA (\
                year smallint(6),\
                month smallint(6),\
                day smallint(6),\
                hour smallint(6),\
                stationID VARCHAR(8),\
                jd smallint(6),\
                dateTime datetime,\
                SO2 double(12,8),\
                CO double(12,8),\
                O3 double(12,8),\
                PM10 double(12,6),\
                NOx double(12,8),\
                NO double(12,8),\
                NO2 double(12,8),\
                THC double(12,8),\
                NMHC double(14,10),\
                CH4 double(12,8),\
                wind double(12,8),\
                wDir double(12,6),\
                wDirStd double(10,6),\
                wDirGlobal double(10,6),\
                tmp double(10,6),\
                dpt double(10,2),\
                tmpIn double(10,6),\
                pres double(10,6),\
                pH double(10,6),\
                glibRad double(10,6),\
                ubvRad double(10,6),\
                netRad double(10,6),\
                pWatCond double(10,6),\
                pWat double(10,6),\
                pWatI double(10,6),\
                PMf double(10,6),\
                uGrd double(22,18),\
                vGrd double(22,18),\
                vapP double(10,6),\
                rh double(10,6),\
                uvi double(10,6),\
                uba double(10,6),\
                pWatHr double(10,6),\
                remark int(10),\
                tke double(8,6),\
                msrepl_tran_version varchar(38),\
                PMfCorr double(10,6),\
                PRIMARY KEY (year,month,day,hour,stationID),\
                INDEX(stationID),\
                INDEX(dateTime)\
                );"
            cursor.execute(sql)
            
            sql = "CREATE TABLE IF NOT EXISTS PMf_Composition (\
                stationID VARCHAR(255),\
                Site VARCHAR(255),\
                Date DATETIME,\
                Na_ion FLOAT,\
                K_ion FLOAT,\
                Mg_ion FLOAT,\
                Ca_ion FLOAT,\
                SO4_ion FLOAT,\
                NH4_ion FLOAT,\
                NO3_ion FLOAT,\
                Cl_ion FLOAT,\
                OC FLOAT,\
                EC FLOAT,\
                PMf_Mass FLOAT,\
                Al FLOAT,\
                Fe FLOAT,\
                Na FLOAT,\
                Mg FLOAT,\
                K FLOAT,\
                Ca FLOAT,\
                Sr FLOAT,\
                Ba FLOAT,\
                Ti FLOAT,\
                Mn FLOAT,\
                Co FLOAT,\
                Ni FLOAT,\
                Cu FLOAT,\
                Zn FLOAT,\
                Mo FLOAT,\
                Cd FLOAT,\
                Sn FLOAT,\
                Sb FLOAT,\
                Tl FLOAT,\
                Pb FLOAT,\
                V FLOAT,\
                Cr FLOAT,\
                Arsenic FLOAT,\
                Y FLOAT,\
                Se FLOAT,\
                Zr FLOAT,\
                Ge FLOAT,\
                Rb FLOAT,\
                Cs FLOAT,\
                Ga FLOAT,\
                La FLOAT,\
                Ce FLOAT,\
                Pr FLOAT,\
                Nd FLOAT,\
                Sm FLOAT,\
                Eu FLOAT,\
                Gd FLOAT,\
                Tb FLOAT,\
                Dy FLOAT,\
                Ho FLOAT,\
                Er FLOAT,\
                Tm FLOAT,\
                Yb FLOAT,\
                Lu FLOAT,\
                Hf FLOAT,\
                U FLOAT,\
                PRIMARY KEY (Site,Date),\
                INDEX(Date)\
                );"
            cursor.execute(sql)

        self.connection.commit()
        
        
    def CollectData(self):
        #fetch aqi sites
        sites = {}
        with self.connection.cursor() as cursor:
            sql = "select StationID,StationName from epa_stationID"
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                sites[row["StationName"]] = row["StationID"]
                
        print("Collect EPBData")
        r = requests.get("http://opendata.epa.gov.tw/ws/Data/ATM00769/?$format=json")
        if r.status_code == requests.codes.all_okay:
            data = r.json()
            dataGroup = {}
            for d in data:
                pad = util.PadLeft(d["SiteId"],"0",3)
                d["SiteId"] = "EPB"+pad
                d["MonitorDate"] = d["MonitorDate"].replace("上午","AM")
                d["MonitorDate"] = d["MonitorDate"].replace("下午","PM")
                date = datetime.datetime.strptime(d["MonitorDate"], '%Y/%m/%d %p %I:%M:%S')
                d["MonitorDate"] = date.strftime("%Y-%m-%d %H:%M:%S")
                #print(d)
                if d["SiteId"] not in dataGroup:
                    dataGroup[d["SiteId"]] = {}
                if d["MonitorDate"] not in dataGroup[d["SiteId"]]:
                    dataGroup[d["SiteId"]][d["MonitorDate"]] = {}
                dataGroup[d["SiteId"]][d["MonitorDate"]][d["ItemEngName"]] = d["Concentration"]
                util.DataToDB(self.connection,"EPBData",d)
            
            #print(dataGroup)
            for site in dataGroup:
                for date in dataGroup[site]:
                    src = dataGroup[site][date]
                    t = date.split(" ")
                    ymd = t[0].split("-")
                    hms = t[1].split(":")
                    d = {}
                    d["year"] = ymd[0]
                    d["month"] = ymd[1]
                    d["day"] = ymd[2]
                    d["hour"] = hms[0]
                    d["stationID"] = site
                    d["datetime"] = date
                    if "MNHC" in src:
                        d["NMHC"] = src["NMHC"]
                    if "AMB_TEMP" in src:
                        d["tmp"] = src["AMB_TEMP"]
                    if "CH4" in src:
                        d["CH4"] = src["CH4"]
                    if "O3" in src:
                        d["O3"] = src["O3"]
                    if "PM10" in src:
                        d["PM10"] = src["PM10"]
                    if "PM2.5" in src:
                        d["PMf"] = src["PM2.5"]
                    if "RH" in src:
                        d["rh"] = src["RH"]
                    if "THC" in src:
                        d["THC"] = src["THC"]
                    if "WIND_DIREC" in src:
                        d["wDir"] = src["WIND_DIREC"]
                    if "WIND_SPEED" in src:
                        d["wind"] = src["WIND_SPEED"]
                    if "wind" in d and "dDir" in d:
                        d["uGrd"] = (-1)*d["wind"]*math.sin(d["wDir"]/180*3.1415926)
                        d["vGrd"] = (-1)*d["wind"]*math.cos(d["wDir"]/180*3.1415926)
                    if "CO" in src:
                        d["CO"] = src["CO"]
                    if "NO" in src:
                        d["NO"] = src["NO"]
                    if "NO2" in src:
                        d["NO2"] = src["NO2"]
                    if "NOx" in src:
                        d["NOx"] = src["NOx"]
                    if "SO2" in src:
                        d["SO2"] = src["SO2"]
                    if "PRESSURE" in src:
                        d["pres"] = src["PRESSURE"]
                    util.DataToDB(self.connection,"epb_DATA",d)

            self.connection.commit()
                
                
        r = requests.get("http://opendata.epa.gov.tw/webapi/Data/ATM00756/?$skip=0&$top=1000&format=json")
        if r.status_code == requests.codes.all_okay:
            data = r.json()
            for d in data:
                #print(d)
                d["stationID"] = sites[d["Site"]]
                d["Arsenic"] = d["As"]
                d["PMf_Mass"] = d["PM2.5_Mass_Concentration"]
                date = datetime.datetime.strptime(d["Date"], '%d-%b-%y')
                d["Date"] = date.strftime("%Y-%m-%d %H:%M:%S")
                d.pop('As', None)
                d.pop('PM2.5_Mass_Concentration', None)
                
                util.DataToDB(self.connection,"PMf_Composition",d)
                
            self.connection.commit()