import argparse
import subprocess
import My_RIPE
from My_RIPE import Measurement, Ping_Data, Measurement_Data

# download_tw_hdfs2.py
# modified version of /home/zsb739/code/libs/ripe-measurement-downloader/download.py

def parse_args():
    parser = argparse.ArgumentParser(
            description='Download daily RIPE data for the provided '
                        'measurement ID number to local file system (NOT hadoop)')
    parser.add_argument('start_time', type=int,
                    help="The start time to begin downloading data from, expressed "
                         "as an epoch timestamp")
    parser.add_argument('end_time', type=int, 
                    help="The end time to download data through, expressed "
                         "as an epoch timestamp")
    parser.add_argument('measurement', type=int, nargs="+",
                    help="The integer identification number for the desired "
                         "measurement")
    parser.add_argument('--summaries', type=str,
                    help="A local file containing dictionary of measurment_id : measurement_summary.  Faster than requesting from web.  If omitted, summaries are obtained from web.")
    return parser.parse_args()


# Call the script by typing:
#	$ python download_time_window.py start_time end_time [measurement_id1, measurement_id2, ...]
# where arguments 2 and onward is a ripe atlas measurement id
if __name__ == "__main__":
    args = parse_args()
    # get time stamps
    start_time = args.start_time
    end_time = args.end_time
    summaries_file = args.summaries

    if (end_time - start_time) > 60*60*24:
        # if time window is longer than 1 day
        # for each day in the time window
        for t1,t2 in My_RIPE.days(start_time, end_time):
            # loop through measurement ids
            for measurement_id in args.measurement:
                measurement = Measurement(measurement_id, summaries_file)
                measurement._fetch_to_local(t1, t2)
    else:
        # if time windows is less than 1 day
        # loop through measurement ids
        for measurement_id in args.measurement:
            measurement = Measurement(measurement_id, summaries_file)
            measurement._fetch_to_local(start_time, end_time)

