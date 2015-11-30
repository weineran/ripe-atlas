from ripe.atlas.sagan import PingResult, DnsResult, TracerouteResult, SslResult, Result
import argparse, sys, json
import pprint
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2

summaries = {10001 : {'af': 4,
			'all_scheduling_requests_fulfilled': True,
			'creation_time': 1285891200,
			'description': None,
			'dst_addr': '193.0.14.129',
			'dst_asn': None,
			'dst_name': 'k.root-servers.net',
			'interval': 1800,
			'is_oneoff': False,
			'is_public': True,
			'msm_id': 10001,
			'participant_count': 0,
			'resolve_on_probe': False,
			'resolved_ips': None,
			'result': '/api/v1/measurement/10001/result/',
			'spread': None,
			'start_time': 1285891200,
			'status': {'id': 2, 'name': 'Ongoing'},
			'stop_time': 1577836800,
			'type': {'af': 4, 'id': 6, 'name': 'dns'}},
			1001 : {'af': 4,
			 'all_scheduling_requests_fulfilled': True,
			 'creation_time': 1285891200,
			 'description': None,
			 'dst_addr': '193.0.14.129',
			 'dst_asn': None,
			 'dst_name': 'k.root-servers.net',
			 'interval': 240,
			 'is_oneoff': False,
			 'is_public': True,
			 'msm_id': 1001,
			 'participant_count': 0,
			 'resolve_on_probe': False,
			 'resolved_ips': None,
			 'result': '/api/v1/measurement/1001/result/',
			 'spread': None,
			 'start_time': 1285891200,
			 'status': {'id': 2, 'name': 'Ongoing'},
			 'stop_time': 1577836800,
			 'type': {'af': 4, 'id': 1, 'name': 'ping'}},
			 5004 : {'af': 4,
			 'all_scheduling_requests_fulfilled': True,
			 'creation_time': 1285891200,
			 'description': None,
			 'dst_addr': '192.5.5.241',
			 'dst_asn': None,
			 'dst_name': 'f.root-servers.net',
			 'interval': 1800,
			 'is_oneoff': False,
			 'is_public': True,
			 'msm_id': 5004,
			 'participant_count': 0,
			 'resolve_on_probe': False,
			 'resolved_ips': None,
			 'result': '/api/v1/measurement/5004/result/',
			 'spread': None,
			 'start_time': 1285891200,
			 'status': {'id': 2, 'name': 'Ongoing'},
			 'stop_time': 1577836800,
			 'type': {'af': 4, 'id': 2, 'name': 'traceroute'}}
			}

RIPE_SUMMARY_URL = "https://atlas.ripe.net/api/v1/measurement/%(measurement)d"

def parse_args():
    parser = argparse.ArgumentParser(
            description='Download daily RIPE data for the provided '
                        'measurement ID number')
    parser.add_argument('measurement', type=int, nargs="+",
                    help="The integer identification number for the desired "
                         "measurement")
    return parser.parse_args()

def get_measurement_id(file_name):
	with open(file_name) as results:
		first_result = results.readlines()[0]
		parsed_result = Result.get(first_result)
		return parsed_result.measurement_id

def get_measurement_summary(measurement_id):
	try:
		summary = summaries[measurement_id]
	except:
		print("requesting summary from "+RIPE_SUMMARY_URL)
		response = urllib2.urlopen(
	            RIPE_SUMMARY_URL % {'measurement' : measurement_id})
		json_response = response.read().decode('utf-8')
		response.close()
		summary = json.loads(json_response)
	return summary
	#return json_response

def get_measurement_type(measurement_summary):
	return measurement_summary['type']['name']

class Measurement:

	# constructor
	def __init__(self, file_name, measurement_id, measurement_summary, measurement_type):
		self.file_name = file_name
		self.measurement_id = measurement_id
		self.measurement_summary = measurement_summary
		self.measurement_type = measurement_type
		pprint.pprint(self.measurement_summary)

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
		print(pre_indent+pre_indent+"-Answers: ("+str(len(answers))+" total)")
		self._print_answers_nicely(answers)
		print(pre_indent+pre_indent+"-Authorities: ("+str(len(authorities))+" total)")
		self._print_answers_nicely(authorities)
		print(pre_indent+pre_indent+"-Additionals: ("+str(len(additionals))+" total)")
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
		i = 1
		for this_a in answers:
			print(pre_indent+str(i)+") raw data")
			self._print_raw_data_nicely(this_a.raw_data, pre_indent+" ")
			print(pre_indent+str(i)+") parsed data")
			self._print_answer_data(this_a, pre_indent+" ")
			i += 1

	def _print_raw_data_nicely(self, raw_data, pre_indent):
		for k in raw_data:
			print(pre_indent+k+":	"+str(raw_data[k]))

	def _print_answer_data(self, this_answer, pre_indent):
		answer_dict = vars(this_answer)
		for k in answer_dict:
			if k != 'raw_data':
				print(pre_indent+k+": "+str(answer_dict[k]))

class DNS_Measurement(Measurement):

	# no constructor--inherited from base class

	def print_nicely(self, limit):
		with open(self.file_name) as results:
		    i = 0
		    for result in results.readlines():
		        if limit is not None:
		        	if i >= limit:
		        		return
		        parsed_result = DnsResult.get(result)
		        print("PROBE ID:		"+str(parsed_result.probe_id))
		        print("firmware:		"+str(parsed_result.firmware))
		        print("origin:			"+parsed_result.origin)
		        print("measurement type:	"+self.measurement_type)
		        self._print_dns_nicely(parsed_result)
		        print("\n")
		        i +=1

class Ping_Measurement(Measurement):

	# no constructor--inherited from base class

	def print_nicely(self, limit):
		with open(self.file_name) as results:
		    i = 0
		    for result in results.readlines():
		        if limit is not None:
		        	if i >= limit:
		        		return
		        parsed_result = PingResult.get(result)
		        print("PROBE ID:		"+str(parsed_result.probe_id))
		        print("firmware:		"+str(parsed_result.firmware))
		        print("origin:			"+parsed_result.origin)
		        print("measurement type:	"+self.measurement_type)
		        self._print_ping_nicely(parsed_result)
		        print("\n")
		        i +=1

class Traceroute_Measurement(Measurement):

	# no constructor--inherited from base class

	def print_nicely(self, limit):
		with open(self.file_name) as results:
		    i = 0
		    for result in results.readlines():
		        if limit is not None:
		        	if i >= limit:
		        		return
		        parsed_result = TracerouteResult.get(result)
		        print("PROBE ID:		"+str(parsed_result.probe_id))
		        print("firmware:		"+str(parsed_result.firmware))
		        print("origin:			"+parsed_result.origin)
		        print("measurement type:	"+self.measurement_type)
		        self._print_traceroute_nicely(parsed_result)
		        print("\n")
		        i +=1

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
    measurement_id = get_measurement_id(file_name)
    measurement_summary = get_measurement_summary(measurement_id)
    measurement_type = get_measurement_type(measurement_summary)
    if measurement_type == 'dns':
    	a_measurement = DNS_Measurement(file_name, measurement_id, measurement_summary, measurement_type)
    elif measurement_type == 'ping':
    	a_measurement = Ping_Measurement(file_name, measurement_id, measurement_summary, measurement_type)
    elif measurement_type == 'traceroute':
    	a_measurement = Traceroute_Measurement(file_name, measurement_id, measurement_summary, measurement_type)
    else:
    	a_measurement = Measurement(file_name, measurement_id, measurement_summary, measurement_type)
    a_measurement.print_nicely(limit)
    