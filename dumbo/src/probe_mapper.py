from dumbo import main, opt
from dumbo.lib import sumreducer
from aqualab.dumbo.util import *

import json

class ProbeMapper:
    #@aquaflows.lib.parsers.Json
    def __call__(self, key, value):
        while type(value) == str or type(value) == unicode:
            value = json.loads(value)
        try:
            yield value['prb_id'], value
        except KeyError:
            yield 'NO_PROBE_ID', value

def runner(job):
    job.additer(ProbeMapper)

if __name__ == "__main__":
    main(runner)
