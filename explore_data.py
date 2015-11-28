from ripe.atlas.sagan import PingResult, DnsResult, TracerouteResult, SslResult, Result
import argparse, sys, json
import pprint
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2

def parse_args():
    parser = argparse.ArgumentParser(
            description='Download daily RIPE data for the provided '
                        'measurement ID number')
    parser.add_argument('measurement', type=int, nargs="+",
                    help="The integer identification number for the desired "
                         "measurement")
    return parser.parse_args()

class Measurement:
	RIPE_SUMMARY_URL = "https://atlas.ripe.net/api/v1/measurement/%(measurement)d"

	def __init__(self, file_name):
		self.file_name = file_name
		self.measurement_id = self._get_measurement_id(file_name)
		self.measurement_summary = self._get_measurement_summary()
		pprint.pprint(self.measurement_summary)

	def _get_measurement_id(self, file_name):
		with open(file_name) as results:
			first_result = results.readlines()[0]
			parsed_result = Result.get(first_result)
			return parsed_result.measurement_id

	def _get_measurement_summary(self):
		response = urllib2.urlopen(
                self.RIPE_SUMMARY_URL % {'measurement' : self.measurement_id})
		json_response = response.read().decode('utf-8')
		response.close()
		return json.loads(json_response)
		#return json_response

	def print_nicely(self, limit):
		with open(self.file_name) as results:
		    i = 0
		    for result in results.readlines():
		        if limit is not None:
		        	if i >= limit:
		        		return
		        parsed_result = Result.get(result)
		        print("PROBE ID:		"+str(parsed_result.probe_id))
		        print("firmware:		"+str(parsed_result.firmware))
		        print("origin:			"+parsed_result.origin)
		        try:
		        	meas_type = parsed_result.type
		        except:
		        	meas_type = 'not_specified'
		        print("measurement type:	"+meas_type)
		        if meas_type == 'dns' or meas_type == 'not_specified':
		        	self._print_dns_nicely(parsed_result)
		        print("\n")
		        i +=1

	def _print_dns_nicely(self, parsed_result):
		try:
			response_list = parsed_result.responses
		except:
			print("No response list found--so it's not a dns measurement")
			return
		if response_list is None:
			print("response_list is None")
			return

		print("num dns responses:	"+str(len(response_list)))
		for this_response in response_list:
			pre_indent = "	"
			print("--------response_id:		"+str(this_response.response_id))
			print(pre_indent+"destination_address:	"+this_response.destination_address)
			print(pre_indent+"source_address:		"+this_response.source_address)
			print(pre_indent+"response_time (s):	"+str(this_response.response_time))
			self._print_qbuf_nicely(this_response)
			self._print_abuf_nicely(this_response)

	def _print_qbuf_nicely(self, this_response):
		pre_indent = "	"
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
		pre_indent = "	"
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
		print(pre_indent+pre_indent+"-Answers:")
		self._print_answers_nicely(answers)
		print(pre_indent+pre_indent+"-Authorities:")
		self._print_answers_nicely(authorities)
		print(pre_indent+pre_indent+"-Additionals:")
		self._print_answers_nicely(additionals)
		

	def _print_header_nicely(self, buf):
		pre_indent = "		"
		next_indent = pre_indent+"	"
		# print header
		print(pre_indent+"-header: ")
		raw_data = buf.header.raw_data
		self._print_raw_data_nicely(raw_data, next_indent)

	def _print_edns0_nicely(self, buf):
		pre_indent = "		"
		next_indent = pre_indent+"	"
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
		pre_indent = "		"
		next_indent = pre_indent+"	"
		print(pre_indent+"-Questions:	")
		questions = buf.questions 	# the message is guaranteed to have a list of Questions
		for this_q in questions:
			self._print_raw_data_nicely(this_q.raw_data, next_indent)

	def _print_answers_nicely(self, answers):
		pre_indent = "			"
		if answers is None:
			print(pre_indent+"is None")
			return
		for this_a in answers:
			self._print_raw_data_nicely(this_a.raw_data, pre_indent)

	def _print_raw_data_nicely(self, raw_data, pre_indent):
		for k in raw_data:
			print(pre_indent+k+":	"+str(raw_data[k]))

class DNS_Measurement(Measurement):
	

# Call the script by typing:
#	$ python explore_data.py [filename]
# where filename is the name of a file containing a ripe atlas response
if __name__ == "__main__":
    #args = parse_args()
    #file_name = args.measurement[0]
    file_name = sys.argv[1]
    limit = None
    if len(sys.argv) >= 3:
    	limit = int(sys.argv[2])
    a_measurement = Measurement(file_name)
    a_measurement.print_nicely(limit)
    