from ripe.atlas.sagan import Result
import Measurements
from Measurements import Measurement, DNS_Measurement, Measurement_File
from os import listdir
from os.path import isfile, join
import argparse, sys

CSV_FILE_NAME = "dns_qtypes.csv"

def parse_args():
    parser = argparse.ArgumentParser(
            description='Given a directory with RIPE DNS measurement data, gets all the Qtypes of all the measurements and puts them in a CSV.')
    parser.add_argument('csv_path', type=str,
                    help="The directory in which to create the CSV file.  File will be named "+CSV_FILE_NAME+". If the file already exists, it will be overwritten.")
    parser.add_argument('data_path', type=str,
                    help="The path of the directory containing DNS data files to analyze")
    parser.add_argument('summaries_file', type=str, nargs="?",
                    help="A file containing a dictionary of measurement_id : measurement_summary.  If ommitted, summaries are obtained from internet.")
    return parser.parse_args()


# Call the script by typing:
#	$ python explore_data.py filename [limit] [summaries_file]
# where filename is the name of a file containing a ripe atlas response
if __name__ == "__main__":
    args = parse_args()
    summaries_file = args.summaries_file
    csv_file = join(args.csv_path, CSV_FILE_NAME)
    data_path = args.data_path

    list_of_headings = DNS_Measurement.get_qtype_headings()
    Measurements.prep_csv_file(csv_file, list_of_headings)

    list_of_entries = listdir(data_path)
    list_of_files = [f for f in list_of_entries if isfile(join(data_path, f))]

    for this_file in list_of_files:
    	measurement_file = Measurement_File(this_file)
    	measurement_id = measurement_file.get_measurement_id_from_file()
    	if measurement_id == None:
            print(this_file + " appears to be empty.  Skipping.")
            continue
    	measurement_summary = measurement_file.get_measurement_summary_from_file(measurement_id, summaries_file)
    	measurement_type = measurement_file.get_measurement_type_from_summary(measurement_summary)

    	if measurement_type == 'dns':
    		dns_measurement = DNS_Measurement(this_file, measurement_id, measurement_summary, measurement_type)
    		dns_measurement.write_qtypes_to_csv(csv_file)

    