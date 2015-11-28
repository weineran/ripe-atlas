import urllib2, os, json, pytz
import datetime, calendar, pprint
import argparse
import snakebite
import subprocess
from snakebite.client import AutoConfigClient
client = AutoConfigClient()

# download_time_window.py
# modified version of /home/zsb739/code/libs/ripe-measurement-downloader/download.py
# The purpose of this script is to download small data files just to see what
# the data looks like and for testing.
# Specify an epoch  start_time_stamp and end_time_stamp, preferably no more than a couple
# hours apart (1 hour is 3600 seconds)

def parse_args():
    parser = argparse.ArgumentParser(
            description='Download daily RIPE data for the provided '
                        'measurement ID number')
    parser.add_argument('start_time', type=int, nargs="1",
                    help="The start time to begin downloading data from, expressed "
                         "as an epoch timestamp")
    parser.add_argument('end_time', type=int, nargs="1",
                    help="The end time to download data through, expressed "
                         "as an epoch timestamp")
    parser.add_argument('measurement', type=int, nargs="+",
                    help="The integer identification number for the desired "
                         "measurement")
    return parser.parse_args()

# a generator function that generates pairs of t1, t2
# t1 is the timestamp representing the first second of a day
# t2 is the timestamp representing the last second of the day
def days(start, stop=None):
    if stop == None:
        curr_time = datetime.datetime.utcnow()
        stop_time = datetime.datetime(curr_time.year, curr_time.month, 
                                      curr_time.day - 1, 23, 59, 59, tzinfo=pytz.UTC)
    else:
        timestamp = datetime.datetime.fromtimestamp(float(stop))
        stop_time = datetime.datetime(timestamp.year, timestamp.month, 
                                      timestamp.day, 23, 59, 59, tzinfo=pytz.UTC)
        
    timestamp = datetime.datetime.fromtimestamp(float(start), tz=pytz.UTC)
    start_time = datetime.datetime(timestamp.year, timestamp.month, 
                             timestamp.day, 0, 0, 0, tzinfo=pytz.UTC)
    curr_time = start_time
    while curr_time < stop_time:
        t1 = calendar.timegm(curr_time.timetuple())
        t2 = calendar.timegm(
                (curr_time + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)).timetuple())
        yield t1, t2
        curr_time += datetime.timedelta(days=1)
    

class Measurement:
    RIPE_SUMMARY_URL = "https://atlas.ripe.net/api/v1/measurement/%(measurement)d"
    RIPE_RESULTS_URL = "https://atlas.ripe.net/api/v1/measurement/%(measurement)d"\
                       "/result/?start=%(start)d&stop=%(stop)d&format=txt"
    RIPE_DATA_ROOT_DIR = "/data/ripe-atlas"
    def __init__(self, measurement_id):
        self.measurement_id = measurement_id
        self.measurement_summary = self._get_measurement_summary()
        pprint.pprint(self.measurement_summary)
    
    def _get_measurement_summary(self):
        response = urllib2.urlopen(
                self.RIPE_SUMMARY_URL % {'measurement' : self.measurement_id})
        json_response = response.read()
        return json.loads(json_response)
    
    def _hdfs_file_exists(self, file_path):
        try:
            list(client.ls([file_path]))
            return True
        except snakebite.errors.FileNotFoundException:
            return False

    def _put_hdfs(self, filename, location):
        index = location.rfind("/")
        if not self._hdfs_file_exists(location[:index]):
            for index in [i for i in range(len(location)) if location.startswith('/', i)]:
                if index == 0:
                    continue
                subprocess.call(["hadoop", "fs", "-mkdir", location[:index]])
        subprocess.call(["hadoop", "fs", "-copyFromLocal", filename, location[:index]])

    def _fetch_if_missing(self, start, stop):
        tstamp = datetime.datetime.fromtimestamp(start, tz=pytz.UTC)
        filename = tstamp.strftime('%Y-%m-%d-%H-%M') + "_" + \
                    "Ripe.Atlas."+ \
                    self.measurement_summary['type']['name'] + "." + \
                    str(self.measurement_id)
        print(filename)
	
        full_hdfs_path = os.path.join(self.RIPE_DATA_ROOT_DIR,
                                     str(tstamp.year),
                                     "%02d" % tstamp.month,
                                     filename)
        if self._hdfs_file_exists(full_hdfs_path):
           return
	

        if os.path.isfile(filename):
            self._put_hdfs(filename, full_hdfs_path)
            os.remove(filename)
            return

        url = self.RIPE_RESULTS_URL % {'measurement' : self.measurement_id,
                                       'start' : start, 
                                       'stop' : stop}
        measurements = urllib2.urlopen(url)
        with open(filename, "w") as f:
            f.write(measurements.read())
        self._put_hdfs(filename, full_hdfs_path)
        os.remove(filename)
            
    def fetch_all_missing(self):
       start = datetime.datetime.fromtimestamp(float(self.measurement_summary['start_time']))
       stop = self.measurement_summary['stop_time']
       status = self.measurement_summary['status']
       for t1,t2 in days(self.measurement_summary['start_time'], stop):
           self._fetch_if_missing(t1, t2)

    def fetch_missing_day(self, start, stop):
        earliest_measurement = self.measurement_summary['start_time']
        latest_measurement = self.measurement_summary['stop_time']

        if start < earliest_measurement:
            start = earliest_measurement
            print("start time amended to earliest measurement: "+str(start))
        if stop > latest_measurement:
            stop = latest_measurement
            print("stop time amended to latest measurement: "+str(stop))

        self._fetch_if_missing(t1, t2)
    


# Call the script by typing:
#	$ python download_time_window.py start_time end_time [measurement_id1, measurement_id2, ...]
# where arguments 2 and onward is a ripe atlas measurement id
if __name__ == "__main__":
    args = parse_args()
    # get time stamps
    start_time = args.start_time
    end_time = args.end_time
    # for each day in the time window
    for t1,t2 in days(start_time, end_time):
        # loop through measurement ids
        for measurement_id in args.measurement:
            measurement = Measurement(measurement_id)
            measurement._fetch_missing_day(t1, t2)
    
