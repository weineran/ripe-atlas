import urllib2, os, json, pytz, sys
import datetime, calendar, pprint
import argparse
import snakebite
import subprocess
from snakebite.client import AutoConfigClient
client = AutoConfigClient()

# download.py
# modified version of /home/zsb739/code/libs/ripe-measurement-downloader/experiment_launcher/download.py
# This script downloads data from ripe atlas and stores it in the hdfs

def parse_args():
    parser = argparse.ArgumentParser(
            description='Download daily RIPE data for the provided '
                        'measurement ID number')
    parser.add_argument('measurement', type=int, nargs="+",
                    help="The integer identification number for the desired "
                         "measurement")
    return parser.parse_args()


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
        response.close()
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
        filename = tstamp.strftime('%Y-%m-%d') + "_" + \
                    "Ripe.Atlas."+ \
                    self.measurement_summary['type']['name'] + "." + \
                    str(self.measurement_id)
        print filename
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
	print("url: "+url)
        measurements = urllib2.urlopen(url)
        with open(filename, "w") as f:
	        f.write(measurements.read())
        self._put_hdfs(filename, full_hdfs_path)
        os.remove(filename)
        measurements.close()
        
    def fetch_all_missing(self):
        #start = datetime.datetime.fromtimestamp(float(self.measurement_summary['start_time']))
        start = self.measurement_summary['start_time']
        stop = self.measurement_summary['stop_time']
        status = self.measurement_summary['status']
        self._fetch_if_missing(start, stop)
	#test_start = 1447000000 # sun 11/8/15
	#test_stop = test_start + 60*10 # 10 minutes later
	#self._fetch_if_missing(test_start, test_stop)



# Call the script by typing:
#        $ python download.py [filename]
# where each line in [filename] is a json object that looks like this:
#        {"measurements": [1969179, ...]}
# That is, each line is a json object containing a list of ripe atlas measurement IDs
if __name__ == "__main__":
    #args = parse_args()
    import json
    for line in open(sys.argv[1], "r"):
        d = json.loads(line)
        for measurement_id in d['measurements']:
            measurement = Measurement(measurement_id)
            measurement.fetch_all_missing()
            
