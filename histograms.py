import My_RIPE
from My_RIPE import Measurement, Measurement_Data
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import json
from collections import Counter

d = Measurement._get_dict_from_nonempty_file('probe-target_dict_all.txt')

pings_1 = []
pings_2 = []
dns_1 = []
dns_2 = []
traceroutes_1 = []
traceroutes_2 = []

for k in d:
	if len(d[k]['origins']) == 1:
		pings_1.append(d[k]['ping'])
		dns_1.append(d[k]['dns'])
		traceroutes_1.append(d[k]['traceroute'])
	elif len(d[k]['origins']) == 2:
		pings_2.append(d[k]['ping'])
		dns_2.append(d[k]['dns'])
		traceroutes_2.append(d[k]['traceroute'])

out_dict = {}
out_dict['pings_1'] = pings_1
out_dict['pings_2'] = pings_2
out_dict['dns_1'] = dns_1
out_dict['dns_2'] = dns_2
out_dict['traceroutes_1'] = traceroutes_1
out_dict['traceroutes_2'] = traceroutes_2

with open('histogram_dict.txt', 'w') as f:
    json.dump(out_dict, f)

f, ((ax11, ax12, ax13), (ax21, ax22, ax23)) = plt.subplots(2, 3)

ax11.hist(pings_1, 50, facecolor='green')
ax11.set_ylabel('count')
ax11.set_title('pings from 1 origin')

ax12.hist(traceroutes_1, 50, facecolor='green')
ax12.set_ylabel('count')
ax12.set_title('traceroutes from 1 origin')

ax13.hist(dns_1, 50, facecolor='green')
ax13.set_ylabel('count')
ax13.set_title('dns from 1 origin')

ax21.hist(pings_2, 50, facecolor='green')
ax21.set_ylabel('count')
ax21.set_title('pings from 2 origins')

ax22.hist(traceroutes_2, 50, facecolor='green')
ax22.set_ylabel('count')
ax22.set_title('traceroutes from 2 origins')

ax23.hist(dns_2, 50, facecolor='green')
ax23.set_ylabel('count')
ax23.set_title('dns from 2 origins')

p1 = Counter(pings_1)
p2 = Counter(pings_2)
t1 = Counter(traceroutes_1)
t2 = Counter(traceroutes_2)
d1 = Counter(dns_1)
d2 = Counter(dns_2)

print("pings1: "+str(p1.most_common(3)))
print("pings2: "+str(p2.most_common(3)))
print("traceroutes1: "+str(t1.most_common(3)))
print("traceroutes2: "+str(t2.most_common(3)))
print("dns1: "+str(d1.most_common(3)))
print("dns2: "+str(d2.most_common(3)))

plt.show()




