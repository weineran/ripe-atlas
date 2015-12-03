from ripe.atlas.sagan import Result
from Measurements import Measurement, DNS_Measurement, Ping_Measurement, Traceroute_Measurement, Measurement_File
import argparse, sys

def parse_args():
    parser = argparse.ArgumentParser(
            description='Expore downloaded RIPE atlas data by printing it out')
    parser.add_argument('filename', type=str,
                    help="The filename containing the data to print")
    parser.add_argument('limit', type=int, nargs="?",
                    help="The max number of measurements results to print.  If ommitted, all measurement results in the file will be printed.")
    parser.add_argument('summaries_file', type=str, nargs="?",
                    help="A file containing a dictionary of measurement_id : measurement_summary.  If ommitted, summaries are obtained from internet.")
    return parser.parse_args()

# Call the script by typing:
#	$ python explore_data.py filename [limit] [summaries_file]
# where filename is the name of a file containing a ripe atlas response
if __name__ == "__main__":
    args = parse_args()
    file_name = args.filename
    limit = args.limit
    summaries_file = args.summaries_file

    measurement_file = Measurement_File(file_name)
    
    measurement_id = measurement_file.get_measurement_id_from_file()
    measurement_summary = measurement_file.get_measurement_summary_from_file(measurement_id, summaries_file)
    measurement_type = measurement_file.get_measurement_type_from_summary(measurement_summary)
    if measurement_type == 'dns':
    	a_measurement = DNS_Measurement(file_name, measurement_id, measurement_summary, measurement_type)
    elif measurement_type == 'ping':
    	a_measurement = Ping_Measurement(file_name, measurement_id, measurement_summary, measurement_type)
    elif measurement_type == 'traceroute':
    	a_measurement = Traceroute_Measurement(file_name, measurement_id, measurement_summary, measurement_type)
    else:
    	a_measurement = Measurement(file_name, measurement_id, measurement_summary, measurement_type)
    a_measurement.print_nicely(limit)
    