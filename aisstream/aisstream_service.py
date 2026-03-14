import asyncio
from asyncio import TimeoutError
import configparser
from datetime import datetime, timezone, timedelta
import json
import logging
from pathlib import Path
import os
import regex
import sys
import threading
import time
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

#'Dimension': {'A': 6, 'B': 15, 'C': 5, 'D': 3},
#dimension a: the distance in meters from the GPS to the bow
#dimension b: the distance in meters from the GPS to the stern
#dimension c: the distance in meters from the GPS to the port side
#dimension d: the distance in meters from the GPS to the starboard side

#Format of mtdata.json - just the ones with asterisks
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

# PositionReport
# https://aisstream.io/documentation#PositionReport
# {'Message': {'PositionReport': {'Cog': 212.8,
#                                 'CommunicationState': 2289,
#                                 'Latitude': 40.668549999999996,
#                                 'Longitude': -74.14208333333333,
#                                 'MessageID': 1,
#                                 'NavigationalStatus': 0,
#                                 'PositionAccuracy': False,
#                                 'Raim': False,
#                                 'RateOfTurn': 0,
#                                 'RepeatIndicator': 0,
#                                 'Sog': 0,
#                                 'Spare': 0,
#                                 'SpecialManoeuvreIndicator': 0,
#                                 'Timestamp': 41,
#                                 'TrueHeading': 25,
#                                 'UserID': 367186370,
#                                 'Valid': True}},
#  'MessageType': 'PositionReport',
#  'MetaData': {'MMSI': 367186370,
#               'MMSI_String': 367186370,
#               'ShipName': 'HMS LIBERTY         ',
#               'latitude': 40.668549999999996,
#               'longitude': -74.14208333333333,
#               'time_utc': '2026-02-15 17:58:41.310321159 +0000 UTC'}}

#ShipStaticData
# https://aisstream.io/documentation#ShipStaticData
# {'Message': {'ShipStaticData': {'AisVersion': 1,
#                                 'CallSign': 'WDE9268',
#                                 'Destination': 'HOBOKEN <> WFI (NYC)',
#                                 'Dimension': {'A': 1, 'B': 22, 'C': 2, 'D': 2},
#                                 'Dte': False,
#                                 'Eta': {'Day': 0,
#                                         'Hour': 24,
#                                         'Minute': 60,
#                                         'Month': 0},
#                                 'FixType': 15,
#                                 'ImoNumber': 0,
#                                 'MaximumStaticDraught': 2,
#                                 'MessageID': 5,
#                                 'Name': 'EMPIRE STATE        ',
#                                 'RepeatIndicator': 0,
#                                 'Spare': False,
#                                 'Type': 60,
#                                 'UserID': 367415390,
#                                 'Valid': True}},
#  'MessageType': 'ShipStaticData',
#  'MetaData': {'MMSI': 367415390,
#               'MMSI_String': 367415390,
#               'ShipName': 'EMPIRE STATE        ',
#               'latitude': 40.72834,
#               'longitude': -74.02473166666667,
#               'time_utc': '2026-02-15 17:56:43.70794484 +0000 UTC'}}

'''Globals'''

FORMAT='%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG,
                    format=FORMAT,
                    filename='aisstream_service.log',
                    filemode='w',
                    encoding='utf-8')
logger = logging.getLogger()
stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.WARNING)
formatter=logging.Formatter(FORMAT)
stderr_handler.setFormatter(formatter)
logger.addHandler(stderr_handler)

# Create a shared threading.Event object
exit_event = threading.Event()


def main():

    config = configparser.ConfigParser()
    try:
        config_file = Path(Path(__file__).parent.resolve(), 'aisstream_config.ini')
        config.read(config_file)
    except Exception:
        logger.fatal("Cannot read in config", exc_info=True)
        sys.exit(1)

    datafile = get_data_file(config)
        
    if os.path.exists(datafile):
        try:
            with open(datafile, mode='r') as f:
                data = json.load(f)

                logger.info("Read %s", datafile)
                print_data_stats(data)

                purge_data(data)
        except JSONDecodeError as e:
            logger.error("%s is corrupt", datafile)
            os.remove(datafile)
    else:
        logger.info("No existing %s", datafile)
        data = {
            "PositionReport": {},
            "ShipStaticData": {}
        }

    # thread to stop at end time
    t = threading.Thread(
        target=end_timer,
        args=(config,),
        daemon=True
    )
    t.start()
        
    asyncio.run(connect_ais_stream(data, config))

    # t = threading.Thread(
    #     target=start_background_loop,
    #     args=(data,config,),
    #     daemon=True)
    # t.start()

    # # return app
    # t.join()

    logger.debug("End of main()")


# End program at StopTime in odd hours
def end_timer(config):
    end_at_min = int(config["DEFAULT"]["StopTime"])

    now = datetime.now()
    sleep_mins = 0

    if now.hour % 2 == 0:
        sleep_mins += 60

    sleep_mins += end_at_min - now.minute 
    if now.minute >= end_at_min:
        sleep_mins += 60

    delta = timedelta(minutes=sleep_mins)
    end_at_time = now + delta
    end_in_s = (end_at_time - now).total_seconds()
    logger.info("End at %s = %ss from now", end_at_time, end_in_s)

    #time.sleep(end_at_time
    time.sleep(end_in_s)

    logger.info("Done!")

    exit_event.set()
    # sys.exit(0)
    

def get_data_file(config):
    workdir = config["DEFAULT"]["WorkDir"]

    datafile = Path(workdir, 'aisstream.json')
    return datafile
    

def get_mtdata_file(config):
    workdir = config["DEFAULT"]["TargetDir"]

    datafile = Path(workdir, 'mtdata.json')
    return datafile


#     # #-----Dummy add in MARY A. WHALEN
#     # my %mary;
#     mary = {}
#     mary["LAT"] = "40.680700"
#     mary["LONG"] = "-74.012783"
#     mary["SPEED"] = "0"
#     mary["COURSE"] = "47"
#     mary["MMSI"] = "-5227445" #what MarineTraffic uses
#     mary["SHIP_ID"] = "964330" #MarineTraffic
#     mary["SHIPNAME"] = "MARY A. WHALEN"
#     mary["FLAG"] = "US"
#     mary["LENGTH"] = "52.4" #172 ft in meters
#     mary["YEAR_BUILT"] = "1938"
#     mary["STATUS"] = "5" #moored
#     mary["SHIPTYPE"] = "80" #tanker
#     mary["TIMESTAMP"] = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f %z %Z')


# def start_background_loop( data, config):
#     asyncio.run(connect_ais_stream(data, config))


async def connect_ais_stream(data, config):

    api_key = config["DEFAULT"]["ApiKey"]
    datafile = get_data_file(config)
    mtdatafile = get_mtdata_file(config)
    ais_stream_uri = "wss://stream.aisstream.io/v0/stream"
    retry_delay = 5 # Seconds to wait before retrying a failed connection

    # 03:15 - we're just using the time, not the whole thing
    #end_at = datetime.strptime(config["DEFAULT"]["StopTime"], "%H:%M")
    #end_at = int(config["DEFAULT"]["StopTime"])

    #logger.info("We'll end at %s in odd hours", end_at)

    while not exit_event.is_set():
        try:
            logger.debug("Connecting to %s", ais_stream_uri)
            async with websockets.connect(ais_stream_uri, open_timeout=30) as websocket:
                subscribe_message = {"APIKey": api_key,  # Required !
                                     "BoundingBoxes": [[[40.596, -74.169], [40.730, -73.902]]], # Required!
                                     # "FiltersShipMMSI": ["368207620", "367719770", "211476060"], # Optional!
                                     "FilterMessageTypes": ["PositionReport", "ShipStaticData"]} # Optional!

                # logger.info(subscribe_message)
                subscribe_message_json = json.dumps(subscribe_message)
                logger.debug("Sending subscribe message")
                await asyncio.wait_for(websocket.send(subscribe_message_json), timeout=10.0)

                last_write = time.time() #seconds
                last_purge = time.time()
                last_mtdata_write = time.time()

                logger.info("Connected to %s", ais_stream_uri)

                async for message_json in websocket:
                    try:
                        message = json.loads(message_json)
                        message_type = message["MessageType"]

                        mmsi = str(message["MetaData"]["MMSI"])
                    except json.JSONDecodeError:
                        logger.error("Failed to decode JSON message from server.")
                        continue
                    except KeyError as e:
                        logger.error(f"Message missing expected key: {e}")
                        continue

                    cur_time = time.time()

                    logger.debug("Received %s for %s", message_type, mmsi)

                    # with data_lock:
                    data[message_type][mmsi] = message

                    #if time_to_end(end_at):
                    #    write_mtdata(data, mtdatafile)
                    #    write_data(data, datafile)
                    #    return

                    # persist local copy every 10 minutes
                    if (cur_time - last_write) > 600.0:
                        # with data_lock:
                        write_data(data, datafile)
                        last_write = cur_time

                    # purge every 60 seconds
                    if (cur_time - last_purge) > 60.0:
                        # with data_lock:
                        purge_data(data)
                        last_purge = cur_time

                    # write out for web every 15 seconds
                    if (cur_time - last_mtdata_write) > 15.0:
                        # with data_lock:
                        # print_data_stats(data)
                        write_mtdata(data, mtdatafile)
                        last_mtdata_write = cur_time

        except (TimeoutError, asyncio.exceptions.TimeoutError) as e:
            logger.warning(f"Connection timed out: {e}. Retrying...")
            #if time_to_end(end_at):
            #    return
            await asyncio.sleep(5)

        except (ConnectionClosed, WebSocketException) as e:
            logger.warning(f"Connection lost or failed: {e}. Retrying in {retry_delay}s...")
            #if time_to_end(end_at):
            #    return
            await asyncio.sleep(retry_delay)

        except Exception as e:
            logger.error(f"Unexpected error: {e}. Shutting down.", exc_info=True)
            break # Exit on critical non-websocket errors

    # exit event happened
    logger.debug("Exit event signaled")
    write_mtdata(data, mtdatafile)
    write_data(data, datafile)

   
def time_to_end(end_at):
    now = datetime.now()
    if now.hour % 2 == 1:
        if end_at <= now.minute <= end_at + 4:
            logger.info("Time to end!")
            return True

    return False
        
    
def print_data_stats(data):
    for type in data:
        #print(type)
        #subpiece = data[type]
        #print(len(subpiece))
        logger.debug('%s: %d', type, len(data[type]))


#----Purge some old vessels----
#$localtime is the current time
#$gmtime is current time
#loop through old simple data and wipe out anything where:
# a) TIMESTAMP is older than 5 hours & speed was greater than 0.5 kts, or
# b) speed was greater than 2 knts and older than 10 minutes, or
# c) speed <= 0.5 kts and older than 48 hrs
def purge_data(data):
    if not 'PositionReport' in data:
        return

    now = datetime.now(tz=timezone.utc)
    
    ten_minutes = timedelta(minutes=10)
    five_hours = timedelta(hours=5)
    two_days = timedelta(hours=48)

    deletes = []
    
    for mmsi in data['PositionReport']:
        prdata = data['PositionReport'][mmsi]
        
        #"2023-07-08 19:42:33.398935932 +0000 UTC"
        pr_timestamp_str = prdata['MetaData']['time_utc']
        #strip off fraction of second
        pr_timestamp_str = regex.sub(r'\.\d+', '', pr_timestamp_str)
        
        pr_timestamp = datetime.strptime(pr_timestamp_str, '%Y-%m-%d %H:%M:%S %z %Z')
        
        pr_speed = prdata['Message']['PositionReport']['Sog']

        purge = False

        if (now - pr_timestamp) > five_hours and pr_speed > 0.5:
            logger.debug("Purging MMSI %s older than 5 hours (%s) and speed > 0.5 kt (%f)", mmsi, pr_timestamp, pr_speed)
            purge = True
        elif (now - pr_timestamp) > ten_minutes and pr_speed > 2.0:
            logger.debug("Purging MMSI %s older than 10 minutes (%s) and speed > 2.0 kt (%f)", mmsi, pr_timestamp, pr_speed)
            purge = True
        elif pr_speed <= 0.5 and (now - pr_timestamp) > two_days:
            logger.debug("Purging MMSI %s older than 48 hours (%s) and speed <= 0.5 kt (%f)", mmsi, pr_timestamp, pr_speed)
            purge = True

        if purge:
            deletes.append(mmsi)

    for mmsi in deletes:
        del data['PositionReport'][mmsi]


def write_data(data, datafile):
    tempfile = Path(datafile.parent, datafile.name + '.tmp')
    with open(tempfile, mode='w') as df:
        json.dump(data, df)
        logger.debug("Wrote %s", tempfile)
    try:
        tempfile.replace(datafile)
        logger.debug("Wrote new %s", datafile)
    except FileNotFoundError as e:
        logger.error("Error moving %s", datafile)
        


def write_mtdata(data, mtdatafile):
    if not 'PositionReport' in data:
        logger.debug("No data")
        return

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

    tempfile = Path(mtdatafile.parent, mtdatafile.name + '.tmp')
    with open(tempfile, 'w') as f:
        json.dump(newdata, f)
    logger.debug("Wrote new temp %s", tempfile)

    try:
        tempfile.replace(mtdatafile)
        # shutil.move(tempfile, mtdatafile)
        logger.debug("Wrote new %s", mtdatafile)
    except FileNotFoundError as e:
        logger.error("Error moving %s", mtdatafile)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.fatal(f"Unexpected error: {e}. Shutting down.", exc_info=True)
