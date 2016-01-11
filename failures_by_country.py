
from dumbo import main, opt
from aqualab.dumbo.util import *
from aqualab.dumbo import parsers
import os, re, datetime, json
import GeoIP
import My_RIPE
from My_RIPE import Measurement, Ping_Data, Measurement_Data
from ripe.atlas.sagan import Result

geoDir = "/scratch/geoipData"
geoRe = re.compile("GeoLiteCity.(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+).dat")

def absDateDiff(d1, d2):
    if d2 > d1:
        return d2 - d1
    else:
        return d1 - d2

class Mapper:
    def __init__(self):
        """
        Find which GeoLiteCity database to use
        """
        files = {}
        for fname in os.listdir(geoDir):
            m = geoRe.match(fname)
            if not m: continue
            fd = datetime.datetime(int(m.group("year")), 
                    int(m.group("month")), int(m.group("day")))
            files[fd] = os.sep.join([geoDir, fname])
        if not files: raise RuntimeError("no GeoIp data files found in %s" % geoDir)
        useD = max(files)
        useFname = files[useD]
        self.geo = GeoIP.open(useFname, GeoIP.GEOIP_INDEX_CACHE)

    def __call__(self, key, value):
        while type(value) == str or type(value) == unicode:
            value = json.load(value)

	parsed_result = Result.get(value)

        ipStr = parsed_result.origin

        gir = self.geo.record_by_addr(ipStr)
        if gir is None:
            value["geo"] = {}
            yield key, value
            return

        d = {}

        fields = ["country_code", "country_name", "region", "region_name",
                "city", "postal_code", "latitude", "longitude", "area_code",
                "time_zone", "metro_code"]

        saved_fields = ["cc", "country", "st", "state", "city", "postcode",
                "lat", "lng", "areacode", "tz", "metrocode"]

        for field, save_as in zip(fields, saved_fields):
            val = gir[field]
            if val is None: continue
            d[save_as] = val

        value["geo"] = d

        yield key, value

def runner(job):
    job.additer(Mapper)

def starter(prog):
    parseArgs(prog)

if __name__ == "__main__":
    main(runner, starter)

