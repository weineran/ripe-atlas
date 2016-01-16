from dumbo import main, opt
from dumbo.lib import sumreducer
from aqualab.dumbo.util import *

import aquaflows.lib.parsers   

class TimestampReducer:
    @aquaflows.lib.parsers.Json
    def __call__(self, key, values):
        measurement_count = 0
        failure_count = 0
        for timestamp, is_failure, value in values:
            measurement_count += 1
            if is_failure:
                failure_count += 1
        yield key, (measurement_count, failure_count)

        

def runner(job):
    job.additer(TimestampReducer)

if __name__ == "__main__":
    main(runner)
