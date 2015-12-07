from ripe.atlas.sagan import Result
import My_RIPE
from My_RIPE import Measurement, Ping_Data, Measurement_Data
from os import listdir
from os.path import isfile, join
import argparse, sys

CSV_FILE_PREFIX = "pings."

def parse_args():
    parser = argparse.ArgumentParser(
            description='Given a directory with RIPE ping measurement data, puts data into a CSV.')
    parser.add_argument('csv_path', type=str,
                    help="The directory in which to create the CSV file.  File will be named '"+CSV_FILE_PREFIX+"[probe_id].csv'. If the file already exists, it will be overwritten.")
    parser.add_argument('data_path', type=str,
                    help="The path of the directory containing ping data files to analyze")
    parser.add_argument('--probe_id', type=int,
                    help="The ripe atlas Probe_ID.  If ommitted, use all probes.")
    parser.add_argument('summaries_file', type=str, nargs="?",
                    help="A file containing a dictionary of measurement_id : measurement_summary.  If ommitted, summaries are obtained from internet.")
    return parser.parse_args()


# Call the script by typing:
#    $ python explore_data.py filename [limit] [summaries_file]
# where filename is the name of a file containing a ripe atlas response
if __name__ == "__main__":
    args = parse_args()
    summaries_file = args.summaries_file
    data_path = args.data_path
    probe_id = args.probe_id
    csv_file = join(args.csv_path, CSV_FILE_PREFIX+str(probe_id)+".csv")

    list_of_headings = Ping_Data.get_ping_headings()
    Measurement_Data.prep_csv_file(csv_file, list_of_headings)

    list_of_entries = listdir(data_path)
    list_of_files = [join(data_path, f) for f in list_of_entries if isfile(join(data_path, f))]

    for this_file in list_of_files:
        print("reading "+this_file)
        measurement_id = Measurement_Data.get_measurement_id_from_file(this_file)
        if measurement_id == None:
            print(this_file + " appears to be empty.  Skipping.")
            continue

        measurement_data = Measurement_Data(measurement_id, this_file, summaries_file)

        if measurement_data.type == 'ping':
            ping_data = Ping_Data(measurement_id, this_file, summaries_file)
            ping_data.write_data_to_csv(csv_file, probe_id)

    