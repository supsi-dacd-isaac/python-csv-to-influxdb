# --------------------------------------------------------------------------- #
# Importing section
# --------------------------------------------------------------------------- #

import logging
import argparse
import json
import glob
import csv
import pytz
import datetime
import calendar
import sys
import time

from influxdb import InfluxDBClient

# --------------------------------------------------------------------------- #
# Functions
# --------------------------------------------------------------------------- #

# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', help='configuration file')
    arg_parser.add_argument('-l', help='log file')

    args = arg_parser.parse_args()
    config = json.loads(open(args.c).read())

    # --------------------------------------------------------------------------- #
    # Set logging object
    # --------------------------------------------------------------------------- #
    if not args.l:
        log_file = None
    else:
        log_file = args.l

    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)-15s::%(levelname)s::%(funcName)s::%(message)s', level=logging.INFO,
                        filename=log_file)

    # --------------------------------------------------------------------------- #
    # InfluxDB connection
    # --------------------------------------------------------------------------- #
    logger.info("Connection to InfluxDB server on [%s:%s]" % (config['influxdb_connection']['host'],
                                                              config['influxdb_connection']['port']))
    try:
        idb_client = InfluxDBClient(host=config['influxdb_connection']['host'],
                                    port=int(config['influxdb_connection']['port']),
                                    username=config['influxdb_connection']['user'],
                                    password=config['influxdb_connection']['password'],
                                    database=config['influxdb_connection']['db'])
    except Exception as e:
        logger.error("EXCEPTION: %s" % str(e))
        sys.exit(2)
    logger.info("Connection successful")

    # --------------------------------------------------------------------------- #
    # Starting program
    # --------------------------------------------------------------------------- #
    logger.info("Starting program")
    influxdb_data_points = []
    for i in range(0, len(config['csv_parameters'])):
        tz_local = pytz.timezone(config['csv_parameters'][i]['time_zone'])
        logger.info('Get data from files in %s' % config['csv_parameters'][i]['csv_folder'])
        for csv_file in glob.glob('%s/%s' % (config['csv_parameters'][i]['csv_folder'],
                                             config['csv_parameters'][i]['csv_filter'])):
            logger.info(csv_file)
            with open(csv_file) as csvfile:
                reader = csv.reader(csvfile, delimiter=config['csv_parameters'][i]['delimiter'])
                cnt_lines = 0
                header = None
                for row in reader:
                    if cnt_lines == int(config['csv_parameters'][i]['header_row']):
                        header = row
                    elif cnt_lines > int(config['csv_parameters'][i]['header_row']):
                        # Time management
                        naive_time = datetime.datetime.strptime(row[0], config['csv_parameters'][i]['time_format'])
                        if config['csv_parameters'][i]['time_daylight_saving'] == 'True':
                            local_dt = tz_local.localize(naive_time, is_dst=True)
                        else:
                            local_dt = tz_local.localize(naive_time)
                        utc_dt = local_dt.astimezone(pytz.utc)

                        # Data management
                        num_cols = min([len(header),
                                        int(config['csv_parameters'][i]['first_data_columns_to_consider'])+1])
                        for j in range(1, num_cols):
                            arr_labels = header[j].split(config['csv_parameters'][i]['header_tags']['separator'],
                                                         maxsplit=len(config['csv_parameters'][i]['header_tags']['labels']))
                            # Tags section
                            tags = config['csv_parameters'][i]['static_tags']
                            for k in range(0, len(config['csv_parameters'][i]['header_tags']['labels'])):
                                tags[config['csv_parameters'][i]['header_tags']['labels'][k]] = arr_labels[k]
                            real_tags = dict()
                            for k in tags:
                                real_tags[k] = tags[k]

                            # Field section
                            try:
                                field = {config['csv_parameters'][i]['field']: float(row[j])}
                            except Exception as e:
                                field = {config['csv_parameters'][i]['field']: float(-999)}

                            # Build point section
                            point = {
                                        'time': int(calendar.timegm(datetime.datetime.timetuple(utc_dt))),
                                        'measurement': config['influxdb_connection']['measurement'],
                                        'fields': field,
                                        'tags': real_tags
                                    }
                            influxdb_data_points.append(point)

                            if len(influxdb_data_points) >= int(config['influxdb_connection']['max_lines_per_insert']):
                                logger.info('Sent %i points to InfluxDB server' % len(influxdb_data_points))
                                idb_client.write_points(influxdb_data_points,
                                                        time_precision=config['influxdb_connection']['time_precision'])

                                influxdb_data_points = []
                                logger.info('Wait for %s seconds' %
                                            config['influxdb_connection']['sec_sleep_after_insert'])
                                time.sleep(int(config['influxdb_connection']['sec_sleep_after_insert']))

                    cnt_lines += 1

    if len(influxdb_data_points) > 0:
        logger.info('Sent %i points to InfluxDB server' % len(influxdb_data_points))
        idb_client.write_points(influxdb_data_points, time_precision=config['influxdb_connection']['time_precision'])
    logger.info("Ending program")

#"time","INVx_TempSecPower","INVx_dailyEnergy","INVx_IacL1","INVx_IacL2","INVx_IacL3","INVx_Pac"
