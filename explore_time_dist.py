from ripe.atlas.sagan import Result
import My_RIPE
from My_RIPE import Measurement, Ping_Data, Traceroute_Data, DNS_Data, Measurement_Data
from os import listdir
from os.path import isfile, join
import argparse, sys


def parse_args():
    parser = argparse.ArgumentParser(
            description='Given a directory with RIPE measurement data, display plot(s) showing the distribution in time of the measurement timestamps.')
    parser.add_argument('data_path', type=str,
                    help="The path of the directory containing data files to analyze")
    parser.add_argument('out_file', type=str,
                    help="The path and filename to write the output data to")
    parser.add_argument('--probe_id', type=int,
                    help="The ripe atlas Probe_ID.  If ommitted, use all probes.")
    parser.add_argument('summaries_file', type=str, nargs="?",
                    help="A file containing a dictionary of measurement_id : measurement_summary.  If ommitted, summaries are obtained from internet.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    summaries_file = args.summaries_file
    data_path = args.data_path
    probe_id = args.probe_id
    out_file = args.out_file

    list_of_entries = listdir(data_path)
    list_of_files = [join(data_path, f) for f in list_of_entries if isfile(join(data_path, f))]

    for this_file in list_of_files:
        print("reading "+this_file)

        # Get measurement ID
        measurement_id = Measurement_Data.get_measurement_id_from_file(this_file)
        if measurement_id == None:
            print(this_file + " appears to be empty.  Skipping.")
            continue    # go to next file

        # Get data from this file
        measurement_data = Measurement_Data(measurement_id, this_file, summaries_file)

        if measurement_data.type == 'ping':
            ping_data = Ping_Data(measurement_id, this_file, summaries_file)
            ping_data.write_data_to_csv(csv_file, probe_id)

    