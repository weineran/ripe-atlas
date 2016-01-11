import argparse
import snakebite
import subprocess
import Measurements
from Measurements import Measurement, Ping_Measurement, Measurement_File
from snakebite.client import AutoConfigClient
client = AutoConfigClient()

# modified version of /home/zsb739/code/libs/ripe-measurement-downloader/download.py

def parse_args():
    parser = argparse.ArgumentParser(
            description='Download daily RIPE data for the provided '
                        'measurement ID number')
    parser.add_argument('start_time', type=int,
                    help="The start time to begin downloading data from, expressed "
                         "as an epoch timestamp")
    parser.add_argument('end_time', type=int, 
                    help="The end time to download data through, expressed "
                         "as an epoch timestamp")
    parser.add_argument('measurement', type=int, nargs="+",
                    help="The integer identification number for the desired "
                         "measurement")
    return parser.parse_args()


# Call the script by typing:
#	$ python download_time_window.py start_time end_time [measurement_id1, measurement_id2, ...]
# where arguments 2 and onward is a ripe atlas measurement id
if __name__ == "__main__":
    args = parse_args()
    # get time stamps
    start_time = args.start_time
    end_time = args.end_time
    # for each day in the time window
    for t1,t2 in Measurements.days(start_time, end_time):
        # loop through measurement ids
        for measurement_id in args.measurement:
            
            measurement = Measurement(filename, measurement_id, measurement_summary, measurement_type)
            measurement._fetch_missing_day(t1, t2)
    
