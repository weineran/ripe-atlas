from ripe.atlas.sagan import PingResult, DnsResult, TracerouteResult, SslResult, Result
import pprint, json

try:
    import urllib.request as urllib2
except ImportError:
    import urllib2

RIPE_SUMMARY_URL = "https://atlas.ripe.net/api/v1/measurement/%(measurement)d"

class Measurement_File:

	def __init__(self, filename):
		self.filename = filename

	def get_measurement_id_from_file(self):
		with open(self.filename) as results:
			first_result = results.readlines()[0]
			parsed_result = Result.get(first_result)
			return parsed_result.measurement_id

	def get_measurement_summary_from_file(self, measurement_id, summary_file):
		summaries_dict = self._get_summaries_dict_from_file(summary_file)

		try:
			summary = summaries_dict[str(measurement_id)]    # json automatically converts int keys to strings
		except:
			print("requesting summary from "+RIPE_SUMMARY_URL)
			response = urllib2.urlopen(
		            RIPE_SUMMARY_URL % {'measurement' : measurement_id})
			json_response = response.read().decode('utf-8')
			response.close()
			summary = json.loads(json_response)
			self._add_summary_to_file(measurement_id, summary, summary_file)
		return summary
		#return json_response

	def get_measurement_type_from_summary(self, measurement_summary):
		return measurement_summary['type']['name']

	def _get_summaries_dict_from_file(self, summary_file):
		try: 
			with open(summary_file, 'r') as f:
				summaries_dict = json.load(f)
		except:
			print("unable to load summaries from file.  returning empty dictionary.")
			summaries_dict = {}
		return summaries_dict

	def _add_summary_to_file(self, measurement_id, summary, summary_file):
		summaries_dict = self._get_summaries_dict_from_file(summary_file)
		summaries_dict[str(measurement_id)] = summary    # the conversion of the key from int to string would happen automatically regardless
		with open(summary_file, 'w') as f:
			json.dump(summaries_dict, f)


class Measurement:

	# constructor
	def __init__(self, filename, measurement_id, measurement_summary, measurement_type):
		self.filename = filename
		self.measurement_id = measurement_id
		self.measurement_summary = measurement_summary
		self.measurement_type = measurement_type
		pprint.pprint(self.measurement_summary)

	def print_nicely(self, limit):
		with open(self.filename) as results:
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
		with open(self.filename) as results:
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

	def write_qtypes_to_csv(self, csv_file):
		list_of_qtypes = self._get_list_of_qtypes()
		self._write_qtypes_rows(csv_file, self.filename, self.measurement_id, list_of_qtypes)

	def _get_list_of_qtypes(self):
		list_of_qtypes = []

		with open(self.filename) as results:
			for this_result in results:
				parsed_result = DnsResult.get(this_result)
				response_list = parsed_result.responses

				for this_response in response_list:
					this_abuf = this_response.abuf
					list_of_questions = this_abuf.questions
					for this_question in list_of_questions:
						this_qtype = this_question.type
						list_of_qtypes.append(this_qtype)

		return list_of_qtypes

	def _write_qtypes_rows(self, csv_file, filename, measurement_id, list_of_qtypes):
		sep = ','
		nl = '\n'
		with open(csv_file, 'a') as f:
			for this_qtype in list_of_qtypes:
				f.write(filename + sep + str(measurement_id) + sep + this_qtype + nl)


class Ping_Measurement(Measurement):

	# no constructor--inherited from base class

	def print_nicely(self, limit):
		with open(self.filename) as results:
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
		with open(self.filename) as results:
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