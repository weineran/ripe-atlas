#!/usr/bin/env python

from operator import itemgetter
import sys

current_key = None
current_sent_count = 0
current_received_count = 0
key = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    try:
        key, packets_sent, packets_received = line.split('\t', 2)
    except ValueError:
	# ignore lines that are output as side effects such as warnings
	# silently ignore/discard this line
	continue

    # convert counts (currently a string) to int
    try:
        packets_sent = int(packets_sent)
        packets_received = int(packets_received)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: key) before it is passed to the reducer
    if current_key == key:
	# if we're on the same key, we increment
        current_sent_count += packets_sent
        current_received_count += packets_received
    else:
	# if we're on a new key, print the results of the prior one...
        if current_key:
	    packets_lost = current_sent_count - current_received_count
	    if current_sent_count is 0:
		loss_rate = None
	    else:
		loss_rate = packets_lost / current_sent_count
            # write result to STDOUT
            print '%s\t%s\t%s\t%s' % (current_key, current_sent_count, current_received_count, loss_rate)
	# and initialize values for next key
        current_sent_count = packets_sent
	current_received_count = packets_received
        current_key = key

# outside of for loop now
# do not forget to output the last key if needed!
if current_key == key:
    packets_lost = current_sent_count - current_received_count
    if current_sent_count is 0:
	loss_rate = None
    else:
	loss_rate = packets_lost / current_sent_count
    print '%s\t%s\t%s\t%s' % (current_key, current_sent_count, current_received_count, loss_rate)
