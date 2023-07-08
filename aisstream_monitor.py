import asyncio
import configparser
import websockets
import json
from pathlib import Path
import logging
import pprint
from datetime import datetime, timezone
import shutil
import sys
import time



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
datafile = Path('aisstream.json')
config = configparser.ConfigParser()

logger = logging.getLogger('logger')


def main():
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    config.read('aisstream_api_key.ini')

    api_key = config['DEFAULT']['APIKey']

    data = {}

    if datafile.exists():
        with open(datafile, 'r') as f:
            data = json.loads(f.read())

            logger.info(f"Read {datafile}")
            print_data_stats(data)
        
    
    while True:
        try:
            asyncio.run(asyncio.run(connect_ais_stream(api_key, data)))
        except ConnectionResetError as err:
            secs = 10
            logger.error("Connection error:", err)
            logger.info(f"Sleeping {secs} s....")
            time.sleep(secs)
            logger.info(f"Trying again...")


def print_data_stats(data):
    for type in data:
        logger.info(f"{type}: {len(data[type])}")


def write_data(data):
    tempfile = Path(datafile.name + '.tmp')
    with open(tempfile, 'w') as df:
        json.dump(data, df)
        logger.info(f"Wrote {tempfile}")
        
    shutil.move(tempfile, datafile)
    print_data_stats(data)
    

async def connect_ais_stream(api_key, data):

    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        subscribe_message = {"APIKey": api_key,  # Required !
                             "BoundingBoxes": [[[40.601, -74.169], [40.792, -73.902]]], # Required!
                             #"FiltersShipMMSI": ["368207620", "367719770", "211476060"], # Optional!
                             "FilterMessageTypes": ["PositionReport",
                                                    #"ExtendedClassBPositionReport",
                                                    "ShipStaticData"]} # Optional!

        subscribe_message_json = json.dumps(subscribe_message)
        await websocket.send(subscribe_message_json)

        last_write = time.time() #seconds

        async for message_json in websocket:
            message = json.loads(message_json)
            message_type = message["MessageType"]
            
            mmsi = message["MetaData"]["MMSI"]
            
            time_utc = message["MetaData"]["time_utc"]

            #print(f"{message_type} {mmsi} {time_utc}")
            
            if not message_type in data: data[message_type] = {}
            
            data[message_type][mmsi] = message

            #pprint.pprint(message)

            if message_type == "PositionReport":
                # the message parameter contains a key of the message type which contains the message itself
                ais_message = message['Message']['PositionReport']

                
                
                #print(f"[{datetime.now(timezone.utc)}] ShipId: {ais_message['UserID']} Latitude: {ais_message['Latitude']} Latitude: {ais_message['Longitude']}")
            if message_type == "ExtendedClassBPositionReport":
                # the message parameter contains a key of the message type which contains the message itself
                #ais_message = message['Message']['PositionReport']

                
                
                #print(f"[{datetime.now(timezone.utc)}] ShipId: {ais_message['UserID']} Latitude: {ais_message['Latitude']} Latitude: {ais_message['Longitude']}")
                pass

            cur_time = time.time()
            
            if (cur_time - last_write) > 30.0:
                write_data(data)
                last_write = cur_time
            
if __name__ == "__main__":
    main()
