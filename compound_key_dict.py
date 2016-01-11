from ripe.atlas.sagan import Result
import My_RIPE
from My_RIPE import Measurement, Ping_Data, Measurement_Data
from os import listdir
from os.path import isfile, join
import argparse, sys
import json


def parse_args():
    parser = argparse.ArgumentParser(
            description='Given a directory with RIPE ping measurement data, builds a dictionary that summarizes the data and writes it to a file in json format and a csv file.')
    parser.add_argument('json_dict_file', type=str,
                    help="The path and filename to use for the output json dictionary file.  If the file already exists, it will be overwritten.")
    parser.add_argument('csv_file', type=str,
                    help="The path and filename to use for the output csv file.  If the file already exists, it will be overwritten.")
    parser.add_argument('data_path', type=str,
                    help="The path of the directory containing data files to analyze")
    parser.add_argument('key_part1_str', type=str,
                    help="The first part of the compound key to use")
    parser.add_argument('key_part2_str', type=str,
                    help="The second part of the compound key to use")
    parser.add_argument('--probe_id', type=int,
                    help="The ripe atlas Probe_ID.  If ommitted, use all probes.")
    parser.add_argument('summaries_file', type=str, nargs="?",
                    help="A file containing a dictionary of measurement_id : measurement_summary.  If ommitted, summaries are obtained from internet.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    json_dict_file = args.json_dict_file
    csv_file = args.csv_file
    data_path = args.data_path
    key_part1_str = args.key_part1_str
    key_part2_str = args.key_part2_str
    probe_id = args.probe_id
    summaries_file = args.summaries_file

    list_of_headings = [key_part1_str + " ; " + key_part2_str, key_part1_str, key_part2_str, 'ping', 'dns', 'traceroute', 'num_origins', 'origins_list']
    
    list_of_entries = listdir(data_path)
    list_of_files = [join(data_path, f) for f in list_of_entries if isfile(join(data_path, f))]

    results_dict = {}

    # build results_dict
    for this_file in list_of_files:
        print("reading " + this_file)
        measurement_id = Measurement_Data.get_measurement_id_from_file(this_file)

        # handle empty file
        if measurement_id == None:
            print(this_file + " appears to be empty.  Skipping.")
            continue

        # handle nonempty file
        measurement_data = Measurement_Data(measurement_id, this_file, summaries_file)
        measurement_data.add_compound_key_results(probe_id, results_dict, key_part1_str, key_part2_str)

    # write results_dict to file
    with open(json_dict_file, 'w') as f:
            json.dump(results_dict, f)

    # print(Measurement_Data.calc_results_summary(json_dict_file))
    
    Measurement_Data.write_compound_key_dict_to_CSV(list_of_headings, csv_file, json_dict_file)

