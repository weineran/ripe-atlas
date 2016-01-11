#
# Starting from wordcount.py (built-in spark example)
# This file calculates the loss rate for each probe in the data file
#

import sys
import imp
import json

print(sys.version_info)

from pyspark import SparkContext

# Global Variables
NL = '\n'

# Helper Functions
def get_origin_ip(measurement):
    measurement_dict = json.loads(measurement)
    return measurement_dict["from"].encode('utf-8')


def get_sent_rcvd(measurement):
    measurement_dict = json.loads(measurement)
    res = {}

    try:
        res["packets_sent"] = measurement_dict["sent"]
    except KeyError:
        res["packets_sent"] = None
    else:
        isValid_sent = True

    try:
        res["packets_received"] = measurement_dict["rcvd"]
    except KeyError:
        res["packets_received"] = None
    else:
        isValid_rcvd = True

    return res

def list_add(a, b):
    res = []
    for i in range(0, len(a)):
        res[i] = a[i] + b[i]
    return res

def dict_add(dict1, dict2):
    '''
    Given 2 dictionaries of the form {"packets_sent": n1, "packets_received": m1} and {"packets_sent": n2, "packets_received": m2}, 
    sums the packets_sent (n1+n2) and the packets_received (m1+m2) and returns a single dictionary with the sums
    '''
    res = {}
    for k in dict1:
        if dict1[k] == None:
            return dict2        # if there is a None in dict1, throw that meas away
        elif dict2[k] == None:
            return dict1        # if there is a None in dict2, throw that meas away
        else:
            res[k] = dict1[k] + dict2[k]        # otherwise add values
    return res

def calc_loss_rate(packet_dict):
    '''
    @param packet_dict: a dictionary of form {"packets_sent": 3, "packets_received": 2}
    Calculates the loss rate and returns the dictionary {"packets_sent": 3, "packets_received": 2, "packets_lost": 1, "loss_rate": 0.333}
    '''
    try:
        packet_dict["packets_lost"] = packet_dict["packets_sent"] - packet_dict["packets_received"]
    except TypeError:
        packet_dict["packets_lost"] = None

    try:
        packet_dict["loss_rate"] = float(packet_dict["packets_lost"]) / float(packet_dict["packets_sent"])
    except (TypeError, ZeroDivisionError):
        packet_dict["loss_rate"] = None
    
    return packet_dict


# Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: loss_rate_by_probe <data_file> <out_file> <num partitions>")
        exit(-1)
    # http://stackoverflow.com/questions/24686474/shipping-python-modules-in-pyspark-to-other-nodes
#    sc = SparkContext(appName="PythonLossRate", pyFiles=['/home/awp066/downloads/ripe.atlas.sagan-egg/dist/ripe.atlas.sagan-1.1.8-py2.7.egg'])
    sc = SparkContext(appName="PythonLossRate")
    data_filename = sys.argv[1]
    out_filename = sys.argv[2]
    num_partitions = int(sys.argv[3])
    measurements = sc.textFile(data_filename, num_partitions) # returns file as an RDD of strings
    packet_stats = measurements.map(lambda meas: (get_origin_ip(meas), get_sent_rcvd(meas))) \
                  .reduceByKey(dict_add).mapValues(calc_loss_rate)
    output = packet_stats.collect()
    count = packet_stats.count()
    packet_stats.saveAsTextFile(out_filename)
    '''
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
#                loss_rate = round(packets_lost / packets_sent, 4)
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
    '''
    sc.stop()
