#!/usr/bin/env python

import sys
import My_RIPE
from My_RIPE import Measurement, Ping_Data, Measurement_Data
from ripe.atlas.sagan import Result

#input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    result = line.strip()
    # use ripe.atlas.sagan to parse
    parsed_result = Result.get(result)
    try:
        version = parsed_result.af
    except AttributeError:
	# silently skip
	continue
    if version is 4:
        origin = parsed_result.origin.split('.',3)
	origin16 = origin[0] + '.' + origin[1]

    try:
	packets_sent = parsed_result.packets_sent
	packets_received = parsed_result.packets_received
    except AttributeError:
	# not a ping file
	continue
	
    # write the results to STDOUT (standard output);
    # what we output here will be the input for the
    # Reduce step, i.e. the input for reducer.py
    #
    # tab-delimited; the trivial count is 1
    print('%s\t%s\t%s' % (origin16, packets_sent, packets_received))
