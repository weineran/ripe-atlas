from dumbo import main, opt
from dumbo.lib import sumreducer
from aqualab.dumbo.util import *

import json
import aquaflows.lib.parsers
import ripe.atlas.sagan

def detect_ping_failure(value):
    ''' 
    This function takes a dictionary "value" and mutates it.
    It adds 'is_failure', 'packets_sent', and 'loss_rate' to the dictionary
    '''

    value['is_failure'] = None
    try:
        value['packets_lost'] = value['sent'] - value['rcvd']
    except (KeyError, TypeError):
        value['packets_lost'] = None

    try:
        value['loss_rate'] = float(value['packets_lost']) / float(value['sent'])
    except (KeyError, TypeError, ZeroDivisionError):
        value['loss_rate'] = None
    else:
        if value['loss_rate'] >= 0.1:
            value['is_failure'] = True
        else:
            value['is_failure'] = False
   

class TimestampMapper:
    #@aquaflows.lib.parsers.Json
    def __call__(self, key, value):
        while type(value) == str or type(value) == unicode:
            value = json.loads(value)
        detect_ping_failure(value)   # mutates the dictionary
        #value['is_failure'] = None

        try:
            yield (key, value['timestamp']), (value['is_failure'], value)
        except KeyError:
            yield (key, 'NO_TIMESTAMP'), (value['is_failure'], value)

def runner(job):
    job.additer(TimestampMapper)

if __name__ == "__main__":
    main(runner)
