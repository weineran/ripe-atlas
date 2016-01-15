import My_RIPE
from My_RIPE import Measurement, Ping_Data, Measurement_Data
import argparse
import os
import numpy
import matplotlib.pyplot as plt
import scipy
import json
from statsmodels.distributions.empirical_distribution import ECDF
from collections import Counter


def parse_args():
    parser = argparse.ArgumentParser(
            description='Given a dictionary with RIPE measurement data, builds a CDF showing the time between measurements.')
    parser.add_argument('json_dict_files', type=str, nargs="+",
                    help="A the paths and filenames containing data in json format.  e.g. timestamp_probe1.json timestamp_probe22.json ...")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    json_dict_files = args.json_dict_files
    #json_dict_files = json.loads(json_dict_files)

    list_of_time_differences = []
    for dict_file in json_dict_files:
        Measurement_Data.write_compound_key_dict_to_list(dict_file, list_of_time_differences)

    print("num measurements: " + str(len(list_of_time_differences)))
    #list_of_time_differences = [1448020803, 1448020805, 1448020808, 1448020808, 1448020816, 1448020831, 1448020834, 1448020855]

    plt.figure(figsize=(4.5,3.5))
    linedata = ECDF(list_of_time_differences)
    plt.plot(linedata.x, linedata.y, lw=3)
    plt.xlabel("Time between measurements (s)")
    plt.ylabel("CDF of measurements")
    plt.ylim(0,1)
    #plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
    #plt.savefig(os.path.join(full_path, "num_networks.pdf"), bbox_inches="tight")
    #plt.close()
    plt.show()

    num_bins = max(list_of_time_differences) - min(list_of_time_differences)
    plt.hist(list_of_time_differences, num_bins)
    plt.ylabel('Count')
    plt.xlabel('Time delta (s)')
    plt.title('Distribution of time between measurements')
    plt.show()

    counted_data = Counter(list_of_time_differences)

    print("median", numpy.median(list_of_time_differences))
    print("mode", counted_data.most_common(1))
    print( "10th percentile", numpy.percentile(list_of_time_differences, 10))
    print( "20th percentile", numpy.percentile(list_of_time_differences, 20))
    print( "25th percentile", numpy.percentile(list_of_time_differences, 25))
    print( "50th percentile", numpy.percentile(list_of_time_differences, 50))
    print( "75th percentile", numpy.percentile(list_of_time_differences, 75))
    print( "90th percentile", numpy.percentile(list_of_time_differences, 90))
    print( "percentile of 30s", scipy.stats.percentileofscore(list_of_time_differences, 30))

