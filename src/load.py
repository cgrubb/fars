from collections import defaultdict
from glob import glob
import os
from dbfpy import dbf
from pymongo import MongoClient

client=MongoClient()
db = client.fars_test
accidents = db['accidents2001']

datapath = './FARS2001'

filenames = glob('{}/*.dbf'.format(datapath))
files = []
for f in filenames:
    base = os.path.basename(f)
    name = os.path.splitext(base)[0].lower()
    files.append((f,name))

print files

#files = [
#('./FARS2011/accident.dbf','accident'),
#('./FARS2011/cevent.dbf','cevent'),
#('./FARS2011/Distract.dbf','distract'),
#('./FARS2011/DrImpair.dbf','drimpair'),
#('./FARS2011/Factor.dbf','factor'),
#('./FARS2011/Maneuver.dbf','maneuver'),
#('./FARS2011/nmcrash.dbf','nmcrash'),
#('./FARS2011/NMImpair.dbf','nmimpair'),
#('./FARS2011/NMPrior.dbf','nmprior'),
#('./FARS2011/parkwork.dbf','parkwork'),
#]

relate = {}

for fn,table in files:
    print "Processing {}".format(fn)
    dbfile = dbf.Dbf(fn)
    for r in xrange(0, len(dbfile)):
        record = dbfile[r]
        record_dict = {}
        for f in dbfile.fieldNames:
            record_dict[f] = record[f]    
        st_case = record['ST_CASE']
        relate.setdefault(st_case,{}).setdefault(table,[]).append(record_dict)

records = []
for k in relate:
    lat = relate[k]["accident"][0]["LATITUDE"]
    lng = relate[k]["accident"][0]["LONGITUD"]
    relate[k]["location"] = {"type":"Point",
                             "coordinates":[lng,lat]}
    records.append(relate[k])

print "Inserting records..."
for rec in records:
    accidents.insert(rec)    
