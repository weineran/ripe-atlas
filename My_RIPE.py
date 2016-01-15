from ripe.atlas.sagan import PingResult, DnsResult, TracerouteResult, SslResult, Result
import pprint, json, os, pytz
import datetime, calendar
import sys
import snakebite
import subprocess
from snakebite.client import AutoConfigClient
client = AutoConfigClient()

try:
    import urllib.request as urllib2
except ImportError:
    import urllib2


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
    """
    This class represents a RIPE Atlas measurement.  Each instance corresponds to one of the measurements listed here: 
    https://atlas.ripe.net/measurements/
    This class contains methods for downloading data and putting it in hdfs
    """

    # static variables
    RIPE_SUMMARY_URL = "https://atlas.ripe.net/api/v1/measurement/%(measurement)d"
    RIPE_RESULTS_URL = "https://atlas.ripe.net/api/v1/measurement/%(measurement)d"\
                       "/result/?start=%(start)d&stop=%(stop)d&format=txt"
    RIPE_DATA_ROOT_DIR = "/data/ripe-atlas"

    # constructor
    def __init__(self, measurement_id, summaries_file = None):
        self.measurement_id = measurement_id
        self.summary, self.local_hit = self._get_summary(summaries_file)  # local_hit is for testing purposes
        self.type = self._get_measurement_type()

    @staticmethod
    def get_measurement_id_from_file(filename):
        with open(filename) as results:
            try:
                first_result = results.readlines()[0]
            except:
                print("Cannot get measurement_id from file: "+filename+" Skipping this file.")
                return None
            parsed_result = Result.get(first_result)
            return parsed_result.measurement_id

    def fetch_range_to_local_fs(self, start, stop):
        earliest_measurement = self.summary['start_time']
        latest_measurement = self.summary['stop_time']

        if start < earliest_measurement:
            start = earliest_measurement
            print("start time amended to earliest measurement: "+str(start))
        if stop > latest_measurement:
            stop = latest_measurement
            print("stop time amended to latest measurement: "+str(stop))

        # fetch 1 day at a time
        for t1,t2 in days(start, stop):
            filename = self._fetch_to_local(t1, t2)

    def fetch_range_to_hdfs(self, start, stop):
        earliest_measurement = self.summary['start_time']
        latest_measurement = self.summary['stop_time']

        if start < earliest_measurement:
            start = earliest_measurement
            print("start time amended to earliest measurement: "+str(start))
        if stop > latest_measurement:
            stop = latest_measurement
            print("stop time amended to latest measurement: "+str(stop))

        # fetch 1 day at a time
        for t1,t2 in days(start, stop):
            filename = self._fetch_to_hdfs_if_missing(t1, t2)

    def _get_summary(self, summaries_file):
        # get dictionary
        summaries_dict = self._get_dict_from_file(summaries_file)
        
        # get summary
        try:
            summary = summaries_dict[str(self.measurement_id)]
            local_hit = True
        except:
            print("Summary "+str(self.measurement_id) + " not found in " + summaries_file)
            summary = self._get_summary_from_web()
            self._add_summary_to_file(summary, summaries_file)
            local_hit = False

        return summary, local_hit

    def _get_measurement_type(self):
        return self.summary['type']['name']

    @staticmethod
    def _get_dict_from_file(summaries_file):
        try:
            summaries_dict = Measurement._get_dict_from_nonempty_file(summaries_file)
        except:
            # summaries file doesn't exist
            print(summaries_file + " doesn't exist or is empty.  Using empty dictionary.")
            summaries_dict = {}

        return summaries_dict

    @staticmethod
    def _get_dict_from_nonempty_file(summaries_file):
        with open(summaries_file, 'r') as f:
            summaries_dict = json.load(f)
        return summaries_dict

    def _get_summary_from_web(self):
        this_url = self.RIPE_SUMMARY_URL % {'measurement' : self.measurement_id}
        print("Requesting summary from "+this_url)
        response = urllib2.urlopen(this_url)
        json_response = response.read().decode('utf-8')
        response.close()
        summary = json.loads(json_response)
        return summary

    def _add_summary_to_file(self, summary, summaries_file):
        print("Adding summary to file.")
        summaries_dict = self._get_dict_from_file(summaries_file)
        summaries_dict[str(self.measurement_id)] = summary    # the conversion of the key from int to string would happen automatically regardless
        with open(summaries_file, 'w') as f:
            json.dump(summaries_dict, f)

    def _fetch_to_local(self, start, stop):
        tstamp = datetime.datetime.fromtimestamp(start, tz=pytz.UTC)
        filename = tstamp.strftime('%Y-%m-%d-%H-%M') + "_" + \
                    "Ripe.Atlas."+ \
                    self.summary['type']['name'] + "." + \
                    str(self.measurement_id)

        # if file exists
        if os.path.isfile(filename):
            # assume it's an incomplete download and remove it
            os.remove(filename)

        #print(start)
        #print(type(start))
        #print(stop)
        #print(type(stop))

        url = self.RIPE_RESULTS_URL % {'measurement' : self.measurement_id,
                                       'start' : int(start), 
                                       'stop' : int(stop)}
        
        self._download_url_to_file(url, filename)       # download the file from url to the local filesystem
        full_local_path = os.path.join(os.getcwd(), filename)
        return full_local_path

    def _fetch_to_hdfs_if_missing(self, start, stop):
        tstamp = datetime.datetime.fromtimestamp(start, tz=pytz.UTC)
        filename = tstamp.strftime('%Y-%m-%d') + "_" + \
                    "Ripe.Atlas."+ \
                    self.summary['type']['name'] + "." + \
                    str(self.measurement_id)
        full_hdfs_path = os.path.join(self.RIPE_DATA_ROOT_DIR,
                                     str(tstamp.year),
                                     "%02d" % tstamp.month,
                                     filename)

        if self._hdfs_file_exists(full_hdfs_path):
            print(full_hdfs_path+" exists.  Skipping.") 
            return
        else:
            filename = self._fetch_to_local(start, stop)
    
        self._move_to_hdfs(filename, full_hdfs_path)    # move the file to the hdfs
        return full_hdfs_path
        

    def _move_to_hdfs(self, filename, full_hdfs_path):
        self._put_hdfs(filename, full_hdfs_path)    # copy the file to the hdfs
        os.remove(filename)                         # remove the file from the local filesystem


    def _download_url_to_file(self, url, filename):
        print("Requesting "+filename+" from "+url)
        print(datetime.datetime.now())
        fail_count = 0
        did_succeed = False
        max_fail = 3

        # try to download the measurement up to max_fail times
        while fail_count < max_fail and did_succeed == False:
            try:
                measurements = urllib2.urlopen(url)  # requests.get() might be better here
                measurement_content = measurements.read().decode('utf-8')
            except Exception as inst:
                fail_count += 1                     # if we failed, increment count
                print("Fail #"+str(fail_count) + ": sys.exc_info()[0]: "+str(sys.exc_info()[0]))
                print("exception type: "+str(type(inst)))
            else:
                with open(filename, "w") as f:
                    f.write(measurement_content)    # writing in chunks might be better, like this http://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py
                did_succeed = True                  # if we get here, we succeeded, breaking while loop


        # if we get here and haven't successfully downloaded the measurement, throw an error
        if did_succeed == False:
            raise RuntimeError('Tried and failed '+str(max_fail)+ ' times to download:\n'+filename+'\n'+'From: '+url)

    def _put_hdfs(self, filename, location):
        index = location.rfind("/")
        if not self._hdfs_file_exists(location[:index]):
            for index in [i for i in range(len(location)) if location.startswith('/', i)]:
                if index == 0:
                    continue
                subprocess.call(["hadoop", "fs", "-mkdir", location[:index]])
        subprocess.call(["hadoop", "fs", "-copyFromLocal", filename, location[:index]])

    def _hdfs_file_exists(self, file_path):
        try:
            list(client.ls([file_path]))
            return True
        except snakebite.errors.FileNotFoundException:
            return False

class Measurement_Data(Measurement, object):

    # STATIC VARIABLES
    SEP = ','
    NL = '\n'

    # constructor
    def __init__(self, measurement_id, filename, summaries_file = None):
        super(Measurement_Data, self).__init__(measurement_id, summaries_file)
        self.filename = filename

    @staticmethod
    def prep_csv_file(csv_file, list_of_headings):
        f = open(csv_file, 'w')
        header = Measurement_Data._build_header(list_of_headings)
        f.write(header)
        f.close()

    @staticmethod
    def _build_header(list_of_headings):
        header = ""
        i = 0
        for this_heading in list_of_headings:
            header += this_heading
            i += 1
            if i != len(list_of_headings):
                header += Measurement_Data.SEP
            else:
                header += Measurement_Data.NL
        return header

    @staticmethod
    def write_dict_to_CSV(list_of_headings, csv_file, output_file):
        results_dict = Measurement._get_dict_from_file(output_file)

        with open(csv_file, "w") as f:
            # headers
            f.write(Measurement_Data._build_header(list_of_headings))

            for key in results_dict:
                f.write(str(key) + ',' + str(results_dict[key]['ping']) + ',' + str(results_dict[key]['dns']) + ',' + str(results_dict[key]['traceroute']) + ',')
                # num_targets
                f.write(str(len(results_dict[key]['targets'])) + ',')
                # targets_list
                for target in results_dict[key]['targets']:
                    f.write(target + ';')
                f.write(',')
                # num_origins
                f.write(str(len(results_dict[key]['origins'])) + ',')
                # origins list
                for origin in results_dict[key]['origins']:
                    f.write(origin + ';')
                f.write('\n')

    # Deprecated
    @staticmethod
    def write_probe_target_dict_to_CSV(list_of_headings, csv_file, output_file):
        results_dict = Measurement._get_dict_from_file(output_file)

        with open(csv_file, "w") as f:
            # headers
            f.write(Measurement_Data._build_header(list_of_headings))
            
            for key in results_dict:
                f.write(str(key) + ',' + str(results_dict[key][list_of_headings[1]]) + ',' + str(results_dict[key][list_of_headings[2]]) + ',' + 
                        str(results_dict[key][list_of_headings[3]]) + ',' + str(results_dict[key][list_of_headings[4]]) + ',' + 
                        str(results_dict[key][list_of_headings[5]]) + ',')
                
                # num_origins
                f.write(str(len(results_dict[key]['origins'])) + ',')
                # origins list
                for origin in results_dict[key]['origins']:
                    f.write(origin + ';')
                f.write('\n')

    @staticmethod
    def write_compound_key_dict_to_CSV(list_of_headings, csv_file, output_file):
        results_dict = Measurement._get_dict_from_file(output_file)

        with open(csv_file, "w") as f:
            # headers
            f.write(Measurement_Data._build_header(list_of_headings))
            
            for key in results_dict:
                f.write(str(key) + ',' + str(results_dict[key][list_of_headings[1]]) + ',' + str(results_dict[key][list_of_headings[2]]) + ',' + 
                        str(results_dict[key][list_of_headings[3]]) + ',' + str(results_dict[key][list_of_headings[4]]) + ',' + 
                        str(results_dict[key][list_of_headings[5]]) + ',')
                
                # num_origins
                f.write(str(len(results_dict[key]['origins'])) + ',')
                # origins list
                for origin in results_dict[key]['origins']:
                    f.write(origin + ';')
                f.write('\n')

    @staticmethod
    def write_compound_key_dict_to_list(json_dict_file, time_diff_list):
        results_dict = Measurement._get_dict_from_file(json_dict_file)
        time_list = []

        # loop through each timestamp in the json dict
        #i = 0
        for key in results_dict:
            timestamp = results_dict[key]['timestamp']

            # get the number of measurements taken at that timestamp
            num_measurements = results_dict[key]['ping'] + results_dict[key]['dns'] + results_dict[key]['traceroute']

            # add that timestamp to the result_list one time for each measurement taken at that timestamp
            for j in range(0, num_measurements):
                time_list.append(timestamp)
                #i += 1

        time_list.sort()

        prev_time = time_list[0]
        for this_time in time_list[1:]:
            time_diff = this_time - prev_time
            time_diff_list.append(time_diff)
            prev_time = this_time

        print("len of time_list: " + str(len(time_list)))
        print("len of time_diff_list: " + str(len(time_diff_list)))
        print("time_diff_list should be 1 shorter than time_list")

        #return time_diff_list


    @staticmethod
    def calc_results_summary(output_file):
        results_summary_dict = {"ping" : {"avg" : None, "max" : None, "min" : None, "total" : 0}, 
                                "dns" : {"avg" : None, "max" : None, "min" : None, "total" : 0},
                                "traceroute" : {"avg" : None, "max" : None, "min" : None, "total" : 0},
                                "num_keys" : 0}

        results_dict = Measurement._get_dict_from_file(output_file)

        for key in results_dict:
            results_summary_dict["num_keys"] += 1
            for inner_key in results_dict[key]:
                # inner_key is ping, traceroute, dns, targets, origins
                this_val = results_dict[key][inner_key]
                curr_max = results_summary_dict[inner_key]["max"]
                curr_min = results_summary_dict[inner_key]["min"]
                if curr_max is None or this_val > curr_max:
                    results_summary_dict[inner_key]["max"] = this_val
                if curr_min is None or this_val < curr_min:
                    results_summary_dict[inner_key]["min"] = this_val
                results_summary_dict[inner_key]["total"] += this_val

        num_keys = results_summary_dict["num_keys"]
        if num_keys != 0:
            results_summary_dict["ping"]["avg"] = results_summary_dict["ping"]["total"] / num_keys
            results_summary_dict["dns"]["avg"] = results_summary_dict["dns"]["total"] / num_keys
            results_summary_dict["traceroute"]["avg"] = results_summary_dict["traceroute"]["total"] / num_keys

        return results_summary_dict

    def add_probe_and_target_results(self, results_dict):
        with open(self.filename) as results:
            for result in results.readlines():
                parsed_result = Result.get(result)
                probe_id = self._get_attr("probe_id", parsed_result)
                target = self._get_attr("target", parsed_result)
                key = str(probe_id) + ' ; ' + target
                origin = parsed_result.origin
                try:
                    # see if this key exists yet
                    inner_dict = results_dict[str(key)]
                except KeyError:
                    # initialize all types to 0
                    results_dict[str(key)] = {"probe_id" : probe_id, "target" : target, "ping" : 0, "dns" : 0, "traceroute" : 0, "origins" : {}}
                    # increment the appropriate type (ping, dns, traceroute)
                    results_dict[str(key)][self.type] += 1
                    results_dict[str(key)]["origins"][origin] = 1
                else:
                    # already initialized, increment
                    results_dict[str(key)][self.type] += 1
                    # origins
                    try:
                        results_dict[str(key)]["origins"][origin] += 1
                    except KeyError:
                        results_dict[str(key)]["origins"][origin] = 1


    def add_compound_key_results(self, probe_id, results_dict, key_part1_str, key_part2_str):
        '''
        key_part1 - the first part of the key (e.g. "probe_id")
        key_part2 - the second part of the key (e.g. "target")
        So the key would be "probe_id ; target"
        '''
        with open(self.filename) as results:
            for result in results.readlines():

                # First determine whether to skip this result
                if probe_id != None:
                    this_probe_id = json.loads(result)["prb_id"]
                    if this_probe_id != probe_id:
                        continue    # skip to next result

                parsed_result = Result.get(result)
                val_part1 = self._get_attr(key_part1_str, parsed_result)
                val_part2 = self._get_attr(key_part2_str, parsed_result)
                key = str(val_part1) + ' ; ' + str(val_part2)
                origin = parsed_result.origin
                try:
                    # see if this key exists yet
                    inner_dict = results_dict[str(key)]
                except KeyError:
                    # initialize all types to 0
                    results_dict[str(key)] = {key_part1_str : val_part1, key_part2_str : val_part2, "ping" : 0, "dns" : 0, "traceroute" : 0, "origins" : {}}
                    # increment the appropriate type (ping, dns, traceroute)
                    results_dict[str(key)][self.type] += 1
                    results_dict[str(key)]["origins"][origin] = 1
                else:
                    # already initialized, increment
                    results_dict[str(key)][self.type] += 1
                    # origins
                    try:
                        results_dict[str(key)]["origins"][origin] += 1
                    except KeyError:
                        results_dict[str(key)]["origins"][origin] = 1


    def add_to_results_dict(self, key_name, results_dict):
        with open(self.filename) as results:
            for result in results.readlines():
                parsed_result = Result.get(result)
                key = self._get_attr(key_name, parsed_result)
                origin = parsed_result.origin
                target = self._get_attr("target", parsed_result)
                try:
                    # see if this key exists yet
                    inner_dict = results_dict[str(key)]
                except KeyError:
                    # initialize all types to 0
                    results_dict[str(key)] = {"ping" : 0, "dns" : 0, "traceroute" : 0, "origins" : {}, "targets" : {}}
                    # increment the appropriate type
                    results_dict[str(key)][self.type] += 1
                    results_dict[str(key)]["origins"][origin] = 1
                    results_dict[str(key)]["targets"][target] = 1
                else:
                    # already initialized, increment
                    results_dict[str(key)][self.type] += 1
                    # origins
                    try:
                        results_dict[str(key)]["origins"][origin] += 1
                    except KeyError:
                        results_dict[str(key)]["origins"][origin] = 1
                    # targets
                    try:
                        results_dict[str(key)]["targets"][target] += 1
                    except KeyError:
                        results_dict[str(key)]["targets"][target] = 1


    def _get_attr(self, this_attr, parsed_result):
        this_attr = this_attr.lower()

        if this_attr == "filename":
            return self.filename
        elif this_attr == "measurement_id":
            return self.measurement_id
        elif this_attr == "probe_id":
            return parsed_result.probe_id
        elif this_attr == "timestamp":
            return parsed_result.created_timestamp
        elif this_attr == "target":
            return self._get_target()
        elif this_attr == "loss_rate":
            try:
                loss_rate = ((parsed_result.packets_sent - parsed_result.packets_received) / parsed_result.packets_sent)
            except ZeroDivisionError:
                loss_rate = None
            return loss_rate
        elif this_attr == "packets_sent":
            return parsed_result.packets_sent
        elif this_attr == "packets_received":
            return parsed_result.packets_received
        elif this_attr == "rtt_avg":
            return parsed_result.rtt_average
        else:
            raise RuntimeError(this_attr + " is not handled in _get_attr() function.")

    def _get_target(self):
        return self.summary['dst_name']

    def print_nicely(self, limit):
        with open(self.filename) as results:
            i = 0
            for result in results.readlines():
                if limit is not None:
                    if i >= limit:
                        return
                parsed_result = Result.get(result)
                print("PROBE ID:        "+str(parsed_result.probe_id))
                print("firmware:        "+str(parsed_result.firmware))
                print("origin:            "+parsed_result.origin)
                try:
                    meas_type = parsed_result.type
                except:
                    meas_type = 'not_specified'
                print("measurement type:    "+meas_type)
                if meas_type == 'dns' or meas_type == 'not_specified':
                    self._print_dns_nicely(parsed_result)
                print("\n")
                i +=1

    def _print_ping_nicely(self, parsed_result):
        ping_dict = vars(parsed_result)
        properties = ['af', 'duplicates', 'rtt_average', 'rtt_median', 'rtt_min', 'rtt_max', 'packets_sent', 'packets_received', 
                        'packet_size', 'destination_name', 'destination_address', 'step']
        for k in properties:
            print(k+": "+str(ping_dict[k]))

        packet_list = parsed_result.packets

        print("packets:")
        i = 0
        for this_packet in packet_list:
            print(" packet "+str(i))
            self._print_ping_packet(this_packet)
            i += 1

    def _print_ping_packet(self, this_packet):
        print("  rtt: "+str(this_packet.rtt))
        print("  dup: "+str(this_packet.dup))
        print("  ttl: "+str(this_packet.ttl))
        print("  source_address: "+str(this_packet.source_address))

    def _print_traceroute_nicely(self, parsed_result):
        traceroute_dict = vars(parsed_result)
        properties = ['af', 'destination_name', 'destination_address', 'source_address', 'end_time', 'end_time_timestamp',
                        'paris_id', 'size', 'protocol', 'total_hops', 'last_rtt', 'destination_ip_responded', 'last_hop_responded']

        for k in properties:
            try:
                print(k+": "+str(traceroute_dict[k]))
            except:
                print(k+" NOT FOUND")
        # hops list

        # ip_path list

    def _print_dns_nicely(self, parsed_result):
        try:
            response_list = parsed_result.responses
        except:
            print("No response list found--so it's not a dns measurement")
            return
        if response_list is None:
            print("response_list is None")
            return

        print("num dns responses:    "+str(len(response_list)))
        for this_response in response_list:
            pre_indent = "    "
            print("--------response_id:        "+str(this_response.response_id))
            print(pre_indent+"destination_address:    "+this_response.destination_address)
            print(pre_indent+"source_address:        "+this_response.source_address)
            print(pre_indent+"response_time (ms):    "+str(this_response.response_time))
            self._print_qbuf_nicely(this_response)
            self._print_abuf_nicely(this_response)

    def _print_qbuf_nicely(self, this_response):
        pre_indent = "    "
        print(pre_indent+"--------query buffer")
        try:
            qbuf = this_response.qbuf
        except:
            print(pre_indent+pre_indent+"no qbuf found")
            return
        if qbuf is None:
            print(pre_indent+"qbuf is 'None'")
            return
        self._print_header_nicely(qbuf)
        self._print_edns0_nicely(qbuf)
        self._print_questions_nicely(qbuf)
        answers = qbuf.answers
        authorities = qbuf.authorities
        additionals = qbuf.additionals
        print(pre_indent+pre_indent+"Answers:")
        self._print_answers_nicely(answers)
        print(pre_indent+pre_indent+"Authorities:")
        self._print_answers_nicely(authorities)
        print(pre_indent+pre_indent+"Additionals:")
        self._print_answers_nicely(additionals)

    def _print_abuf_nicely(self, this_response):
        pre_indent = "    "
        print(pre_indent+"--------answer buffer")
        try:
            abuf = this_response.abuf
        except:
            print(pre_indent+pre_indent+"no abuf found")
            return
        if abuf is None:
            print(pre_indent+"abuf is 'None'")
            return
        self._print_header_nicely(abuf)
        self._print_edns0_nicely(abuf)
        self._print_questions_nicely(abuf)
        answers = abuf.answers
        authorities = abuf.authorities
        additionals = abuf.additionals
        print(pre_indent+pre_indent+"-Answers: ("+str(len(answers))+" total)")
        self._print_answers_nicely(answers)
        print(pre_indent+pre_indent+"-Authorities: ("+str(len(authorities))+" total)")
        self._print_answers_nicely(authorities)
        print(pre_indent+pre_indent+"-Additionals: ("+str(len(additionals))+" total)")
        self._print_answers_nicely(additionals)
        

    def _print_header_nicely(self, buf):
        pre_indent = "        "
        next_indent = pre_indent+"    "
        # print header
        print(pre_indent+"-header: ")
        raw_data = buf.header.raw_data
        self._print_raw_data_nicely(raw_data, next_indent)

    def _print_edns0_nicely(self, buf):
        pre_indent = "        "
        next_indent = pre_indent+"    "
        print(pre_indent+"-edns0: ")
        try:
            edns0 = buf.edns0
        except:
            print(pre_indent+"no edns0 found")
            return
        if edns0 is None:
            print(next_indent+"edns0 is 'None'")
            return
        print(pre_indent+"edns0 exists")
        self._print_raw_data_nicely(edns0.raw_data, next_indent)

    def _print_questions_nicely(self, buf):
        pre_indent = "        "
        next_indent = pre_indent+"    "
        print(pre_indent+"-Questions:    ")
        questions = buf.questions     # the message is guaranteed to have a list of Questions
        for this_q in questions:
            self._print_raw_data_nicely(this_q.raw_data, next_indent)

    def _print_answers_nicely(self, answers):
        pre_indent = "            "
        if answers is None:
            print(pre_indent+"is None")
            return
        i = 1
        for this_a in answers:
            print(pre_indent+str(i)+") raw data")
            self._print_raw_data_nicely(this_a.raw_data, pre_indent+" ")
            print(pre_indent+str(i)+") parsed data")
            self._print_answer_data(this_a, pre_indent+" ")
            i += 1

    def _print_raw_data_nicely(self, raw_data, pre_indent):
        for k in raw_data:
            print(pre_indent+k+":    "+str(raw_data[k]))

    def _print_answer_data(self, this_answer, pre_indent):
        answer_dict = vars(this_answer)
        for k in answer_dict:
            if k != 'raw_data':
                print(pre_indent+k+": "+str(answer_dict[k]))

class DNS_Data(Measurement_Data):

    # no constructor--inherited from base class

    def print_nicely(self, limit):
        with open(self.filename) as results:
            i = 0
            for result in results.readlines():
                if limit is not None:
                    if i >= limit:
                        return
                parsed_result = DnsResult.get(result)
                print("PROBE ID:        "+str(parsed_result.probe_id))
                print("firmware:        "+str(parsed_result.firmware))
                print("origin:            "+parsed_result.origin)
                print("measurement type:    "+self.measurement_type)
                self._print_dns_nicely(parsed_result)
                print("\n")
                i +=1

    def write_qtypes_to_csv(self, csv_file):
        list_of_qtypes = self._get_list_of_qtypes()
        self._write_qtypes_rows(csv_file, self.filename, self.measurement_id, list_of_qtypes)

    def _get_list_of_qtypes(self):
        list_of_qtypes = []

        with open(self.filename) as results:
            for this_result in results:
                parsed_result = DnsResult.get(this_result)
                response_list = parsed_result.responses

                for this_response in response_list:
                    this_abuf = this_response.abuf
                    list_of_questions = this_abuf.questions
                    for this_question in list_of_questions:
                        this_qtype = this_question.type
                        list_of_qtypes.append(this_qtype)

        return list_of_qtypes

    def _write_qtypes_rows(self, csv_file, filename, measurement_id, list_of_qtypes):
        with open(csv_file, 'a') as f:
            for this_qtype in list_of_qtypes:
                f.write(filename + self.SEP + str(measurement_id) + self.SEP + this_qtype + self.NL)

    @staticmethod
    def get_qtype_headings():
        list_of_headings = ["Filename", "Measurement_ID", "Qtype"]
        return list_of_headings


class Ping_Data(Measurement_Data):
    # attributes
    list_of_headings_and_attr = [["Filename", "Measurement_ID", "Probe_ID", "Timestamp",        "Target", "Loss_rate", "Packets_sent", "Packets_received", "RTT_avg"],
                                 [None,        None,            "probe_id", "created_timestamp", None,     None,       "packets_sent", "packets_received", "rtt_average"]]

    # no constructor--inherited from base class

    def print_nicely(self, limit):
        with open(self.filename) as results:
            i = 0
            for result in results.readlines():
                if limit is not None:
                    if i >= limit:
                        return
                parsed_result = PingResult.get(result)
                print("PROBE ID:        "+str(parsed_result.probe_id))
                print("firmware:        "+str(parsed_result.firmware))
                print("origin:            "+parsed_result.origin)
                print("measurement type:    "+self.measurement_type)
                self._print_ping_nicely(parsed_result)
                print("\n")
                i +=1

    @staticmethod
    def get_ping_headings():
        return Ping_Data.list_of_headings_and_attr[0]

    def write_data_to_csv(self, csv_file, probe_id):
        list_of_data = self._get_list_of_data(probe_id)
        self._write_data_rows(csv_file, self.filename, self.measurement_id, list_of_data)

    def _get_list_of_data(self, probe_id):
        list_of_data = []

        with open(self.filename) as results:
            for this_result in results:
                parsed_result = PingResult.get(this_result)
                this_probe_id = parsed_result.probe_id

                # if a probe_id was specified, we only care about that probe's measurements
                if probe_id != None and this_probe_id != probe_id:
                    continue
                
                packets_sent = parsed_result.packets_sent
                packets_received = parsed_result.packets_received
                packets_lost = packets_sent - packets_received
                try:
                    loss_rate = packets_lost/packets_sent
                except ZeroDivisionError:
                    loss_rate = None

                data_row = []
                for this_attr in self.list_of_headings_and_attr[0]:
                    data_row.append(self._get_attr(this_attr, parsed_result))

                list_of_data.append(data_row)

        return list_of_data

    def _write_data_rows(self, csv_file, filename, measurement_id, list_of_data):
        with open(csv_file, 'a') as f:
            for this_data_row in list_of_data:
                i = 0
                for this_item in this_data_row:
                    f.write(str(this_item))
                    i += 1
                    if i != len(this_data_row):
                        f.write(self.SEP)
                    else:
                        f.write(self.NL)


class Traceroute_Data(Measurement_Data):

    # no constructor--inherited from base class

    def print_nicely(self, limit):
        with open(self.filename) as results:
            i = 0
            for result in results.readlines():
                if limit is not None:
                    if i >= limit:
                        return
                parsed_result = TracerouteResult.get(result)
                print("PROBE ID:        "+str(parsed_result.probe_id))
                print("firmware:        "+str(parsed_result.firmware))
                print("origin:            "+parsed_result.origin)
                print("measurement type:    "+self.measurement_type)
                self._print_traceroute_nicely(parsed_result)
                print("\n")
                i +=1



            
