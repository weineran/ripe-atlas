#
# Starting from wordcount.py (built-in spark example)
# This file calculates the loss rate for each probe in the data file
#

import sys
from operator import add
from ripe.atlas.sagan import PingResult, DnsResult, TracerouteResult, SslResult, Result

from pyspark import SparkContext

# Global Variables
NL = '\n'

# Helper Functions
def get_origin_ip(measurement):
    parsed_result = Result.get(measurement)
    return parsed_result.origin

def get_packets_sent(measurement):
    parsed_result = Result.get(measurement)
    return parsed_result.packets_sent

def get_packets_received(measurement):
    parsed_result = Result.get(measurement)
    return parsed_result.packets_received

def list_add(a, b):
    res = []
    for i in range(0, len(a)):
	res[i] = a[i] + b[i]
    return res

def dict_add(dict1, dict2):
    res = {}
    for k in dict1:
	if dict1[k] == None:
	    return dict2	# if there is a None in dict1, throw that meas away
	elif dict2[k] == None:
	    return dict1	# if there is a None in dict2, throw that meas away
	else:
	    res[k] = dict1[k] + dict2[k]	# otherwise add values
    return res

# Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print >> sys.stderr, "Usage: loss_rate_by_probe <data_file> <out_file> <num partitions>"
        exit(-1)
    sc = SparkContext(appName="PythonLossRate")
    data_filename = sys.argv[1]
    out_filename = sys.argv[2]
    num_partitions = int(sys.argv[3])
    measurements = sc.textFile(data_filename, num_partitions) # returns file as an RDD of strings
    packet_stats = measurements.map(lambda meas: (get_origin_ip(meas), {"packets_sent": get_packets_sent(meas), "packets_received" : get_packets_received(meas)})) \
                  .reduceByKey(dict_add)
    output = packet_stats.collect()
    count = packet_stats.count()
    f = open(out_filename, "w")
    f.write("KEY_COUNT (before None-removal) = " + str(count) + NL)
    count_ex_Nones = 0
    f.write("{")
    is_first = True
    for (source, data_dict) in output:
	# still need to check for any Nones in the dict and omit them
	has_None = False
	for k in data_dict:
	    if data_dict[k] == None:
		has_None = True
		break

	# now we can print
	if has_None == False:
	    if is_first:
		is_first = False
	    else:
		f.write("," + NL)
	    count_ex_Nones += 1
	    packets_sent = data_dict["packets_sent"]
	    packets_received = data_dict["packets_received"]
	    packets_lost = packets_sent - packets_received
	    loss_rate = 0.0
	    try:
#		loss_rate = round(packets_lost / packets_sent, 4)
		loss_rate = float(packets_lost) / float(packets_sent)
		loss_rate_str = "{:.3f}".format(loss_rate)
	    except ZeroDivisionError:
		loss_rate = None
		loss_rate_str = str(loss_rate)
	    f.write("'" + source + "': " + \
		"{'packets_sent': " + str(packets_sent) + ", " + \
		"'packets_received': " + str(packets_received) + ", " + \
		"'packets_lost': " + str(packets_lost) + ", " + \
		"'loss_rate': " + loss_rate_str + "}")

    f.write("}" + NL)
    f.write("KEY_COUNT (after None-removal) = " + str(count_ex_Nones))
    f.close()
    sc.stop()
