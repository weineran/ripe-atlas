import My_RIPE
from My_RIPE import Measurement, Measurement_Data


d = Measurement._get_dict_from_nonempty_file('test_dict_all.out')

total_shorter = 0
total_longer = 0
ipv4_only = 0
ipv6_only = 0
both = 0
neither = 0

# loop through probes
for probe in d:
	ipv4 = 0
	ipv6 = 0
	shorter = 0
	longer = 0

	# loop through origins
	for origin in d[probe]['origins']:
		length = len(origin)
		if length < 4+3 and origin != "":
			shorter += 1
			print(origin)
		elif length >= 4+3 and length <= 3*4+3:
			ipv4 +=1
		elif length > 3*4+3 and length <= 8*4+7:
			ipv6 +=1
		elif length > 8*4+7:
			longer +=1

	total_shorter += shorter
	total_longer += longer

	if ipv4 > 0 and ipv6 > 0:
		both += 1
	elif ipv4 > 0:
		ipv4_only += 1
	elif ipv6 > 0:
		ipv6_only += 1
	else:
		neither = 0

# print results
print("ipv4_only: "+str(ipv4_only))
print("ipv6_only: "+str(ipv6_only))
print("both: "+str(both))
print("neither: "+str(neither))
print("total_longer: "+str(total_longer))
print("total_shorter: "+str(total_shorter))