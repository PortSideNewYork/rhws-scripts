#!/usr/bin/python3

#import asyncio
import codecs
import configparser
from datetime import datetime, timezone, timedelta
import json
import logging
import os
#from pathlib import Path
import pprint
import re
import shutil
import ssl
import sys
import time
import websocket



#'Dimension': {'A': 6, 'B': 15, 'C': 5, 'D': 3},
#dimension a: the distance in meters from the GPS to the bow
#dimension b: the distance in meters from the GPS to the stern
#dimension c: the distance in meters from the GPS to the port side
#dimension d: the distance in meters from the GPS to the starboard side

#Format of mtdata.json
# {
#     "LAST_PORT_TIME":"2021-08-21T20:49:00Z", = N/A
#*     "COURSE":"231", = Message.PositionReport.Cog
#     "DRAUGHT":"108", = Message.ShipStaticData.MaximumStaticDraught
#*     "LONG":"-74.144910", = Message.PositionReport.Longitude
#*     "SHIP_ID":"416682", #MarineTraffic's ID = N/A
#*     "FLAG":"Panama", = N/A
#*     "SHIPNAME":"OOCL BRAZIL", = MetaData.ShipName
#*     "SPEED":"0", #1/10th kt Message.PositionReport.Sog, in kt e.g. 0.3
#     "UTC_SECONDS":"48", ??
#     "IMO":"9495038", = Message.ShipStaticData.ImoNumber
#     "MMSI":"355443000", = MetaData.MMSI
#     "CALLSIGN":"3FRZ3", = Message.ShipStaticData.CallSign
#     "DESTINATION":"US SAV", = Message.ShipStaticData.Destination
#*     "LAT":"40.678040", Message.PositionReport.Latitude
#     "ETA":"2021-09-10T17:00:00Z", = Message.ShipStaticData.Eta
#     "LAST_PORT":"PIRAEUS", = N/A
#     "GRT":"87697", = N/A
#     "WIDTH":"45.6", = Message.ShipStaticData.Dimension.c + Message.ShipStaticData.Dimension.d 
#*     "SHIPTYPE":"71", = Message.ShipStaticData.Type
#*     "TIMESTAMP":"2021-09-08T07:17:47Z", = MetaData.time_utc
#     "DSRC":"TER", ??
#     "LENGTH":"316", = Message.ShipStaticData.Dimension.a + Message.ShipStaticData.Dimension.b
#     "STATUS":"5", = Message.PositionReport.NavigationalStatus
#     "HEADING":"308", = Message.PositionReport.TrueHeading
#     "DWT":"90013", = N/A
#     "YEAR_BUILT":"2010", = N/A 
#     "CURRENT_PORT":"NEW YORK" = N/A
# }

'''Globals'''
datafile = 'aisstream.json'
config = configparser.ConfigParser()

logger = logging.getLogger('logger')


def main():
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    #Set working directory, since we do some file stuff here
    try:
        os.chdir('/home/portside/aisstream')
    except Exception:
        pass #not on Unix
    
    try:
        config.read('aisstream_api_key.ini')
        api_key = config['DEFAULT']['APIKey']
    except Exception:
        logger.fatal("Cannot read in API key")
        sys.exit(1)

    if os.path.exists(datafile):
        with open(datafile, mode='r') as f:
            data = json.load(f)

        logger.info("Read %s", datafile)
        print_data_stats(data)

        purge_data(data)
    
    else:
        logger.info("No existing %s", datafile)
        data = {}
    
    while True:
        try:
            #asyncio.run(asyncio.run(connect_ais_stream(api_key, data)))
            connect_ais_stream(api_key, data)
        except ConnectionResetError as err:
            secs = 10
            logger.error("Connection error: " + str(err))
            logger.debug("Sleeping %d s. ...", secs)
            time.sleep(secs)
            logger.debug("Trying again ...")


def connect_ais_stream(api_key, data):

    my_context = ssl.create_default_context()
    my_context.verify_mode = ssl.CERT_REQUIRED
    my_context.check_hostname = True
    my_context.load_default_certs()

    sock = websocket.WebSocket(sslopt={'context': my_context})
    sock.connect("wss://stream.aisstream.io/v0/stream")

    subscribe_message = {"APIKey": api_key,  # Required !
                         "BoundingBoxes": [[[40.596, -74.169], [40.730, -73.902]]], # Required!
                         #"FiltersShipMMSI": ["368207620", "367719770", "211476060"], # Optional!
                         "FilterMessageTypes": ["PositionReport",
                                                #"ExtendedClassBPositionReport",
                                                "ShipStaticData"]} # Optional!

    subscribe_message_json = json.dumps(subscribe_message)
    sock.send(subscribe_message_json)

    last_write = time.time() #seconds
    last_purge = time.time()
    last_mtdata_write = time.time()

    #lock = asyncio.Lock()

    #async for message_json in websocket:

    while True:
        message_json = sock.recv()
        if message_json is None: break
        
        message = json.loads(codecs.decode(message_json))
        message_type = message["MessageType"]
        
        mmsi = str(message["MetaData"]["MMSI"])
        
        time_utc = message["MetaData"]["time_utc"]

        #print(f"{message_type} {mmsi} {time_utc}")
        #async with lock:
        if not message_type in data:
            logger.debug("Initializing data['%s']", message_type)
            data[message_type] = {}
        
        data[message_type][mmsi] = message

        cur_time = time.time()
        
        '''persist local copy every 10 minutes'''
        if (cur_time - last_write) > 600.0:
            write_data(data)
            last_write = cur_time

        '''purge every 60 seconds'''
        if (cur_time - last_purge) > 60.0:
            purge_data(data)
            last_purge = cur_time

        '''write out for web every 15 seconds'''
        if (cur_time - last_mtdata_write) > 15.0:
            write_mtdata(data)
            last_mtdata_write = cur_time


def print_data_stats(data):
    for type in data:
        #print(type)
        #subpiece = data[type]
        #print(len(subpiece))
        logger.debug('%s: %d', type, len(data[type]))


def write_data(data):
    tempfile = datafile + '.tmp'
    with open(tempfile, mode='w') as df:
        json.dump(data, df)
        logger.debug("Wrote " + tempfile)
        
    shutil.move(tempfile, datafile)
    logger.info("Wrote new %s", datafile)


def write_mtdata(data):
    if not 'PositionReport' in data: return

    newdata = []

    for key in data['PositionReport']:
        prdata = data['PositionReport'][key]
    
        mtdata = {
            'MMSI': key,
            'COURSE': str(round(prdata['Message']['PositionReport']['Cog'])),
            'LONG': str(prdata['Message']['PositionReport']['Longitude']),
            'LAT': str(prdata['Message']['PositionReport']['Latitude']),
            'SHIPNAME': prdata['MetaData']['ShipName'].rstrip(),
            'SPEED': str(round(prdata['Message']['PositionReport']['Sog'] * 10)),
            'TIMESTAMP': prdata['MetaData']['time_utc']
        }
    
        if key in data['ShipStaticData']:
            sdata = data['ShipStaticData'][key]
            mtdata['SHIPTYPE'] = str(sdata['Message']['ShipStaticData']['Type'])
    
        newdata.append(mtdata)
    
    # #-----Dummy add in MARY A. WHALEN
    # my %mary;
    mary = {}
    mary["LAT"] = "40.680700"
    mary["LONG"] = "-74.012783"
    mary["SPEED"] = "0"
    mary["COURSE"] = "47"
    mary["MMSI"] = "-5227445" #what MarineTraffic uses
    mary["SHIP_ID"] = "964330" #MarineTraffic
    mary["SHIPNAME"] = "MARY A. WHALEN"
    mary["FLAG"] = "US"
    mary["LENGTH"] = "52.4" #172 ft in meters
    mary["YEAR_BUILT"] = "1938"
    mary["STATUS"] = "5" #moored
    mary["SHIPTYPE"] = "80" #tanker
    mary["TIMESTAMP"] = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f %z %Z')
    
    newdata.append(mary)

    mtfile = 'mtdata.json'
    with open(mtfile, 'w') as f:
        json.dump(newdata, f)
    logger.debug("Wrote new local %s", mtfile)

    targetdir = '/var/www'
    if os.path.exists(targetdir):
        shutil.copy(mtfile, targetdir)
        logger.debug("Wrote new %s/%s", targetdir, mtfile)


#----Purge some old vessels----
#$localtime is the current time
#$gmtime is current time
#loop through old simple data and wipe out anything where:
# a) TIMESTAMP is older than 5 hours & speed was greater than 0.5 kts, or
# b) speed was greater than 2 knts and older than 10 minutes, or
# c) speed <= 0.5 kts and older than 48 hrs
def purge_data(data):
    if not 'PositionReport' in data: return

    now = datetime.now(tz=timezone.utc)
    
    ten_minutes = timedelta(minutes=10)
    five_hours = timedelta(hours=5)
    two_days = timedelta(hours=48)
    
    for mmsi in data['PositionReport']:
        prdata = data['PositionReport'][mmsi]
        
        #"2023-07-08 19:42:33.398935932 +0000 UTC"
        pr_timestamp_str = prdata['MetaData']['time_utc']
        #strip off fraction of second
        pr_timestamp_str = re.sub(r'\.\d+', '', pr_timestamp_str)
        
        pr_timestamp = datetime.strptime(pr_timestamp_str, '%Y-%m-%d %H:%M:%S %z %Z')
        
        pr_speed = prdata['Message']['PositionReport']['Sog']

        purge = False

        if (now - pr_timestamp) > five_hours and pr_speed > 0.5:
            logger.info("Purging MMSI %s older than 5 hours (%s) and speed > 0.5 kt (%f)", mmsi, pr_timestamp, pr_speed)
            purge = True
        elif (now - pr_timestamp) > ten_minutes and pr_speed > 2.0:
            logger.info("Purging MMSI %s older than 10 minutes (%s) and speed > 2.0 kt (%f)", mmsi, pr_timestamp, pr_speed)
            purge = True
        elif pr_speed <= 0.5 and (now - pr_timestamp) > two_days:
            logger.info("Purging MMSI %s older than 48 hours (%s) and speed <= 0.5 kt (%f)", mmsi, pr_timestamp, pr_speed)
            purge = True

if __name__ == "__main__":
    main()
