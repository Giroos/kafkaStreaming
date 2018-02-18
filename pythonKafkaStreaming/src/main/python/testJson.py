
import ConfigParser
import json

from com.iai.Configuration import Configuration

class FlightEvent(object):

    planeCode = None
    speed = None
    planeCountry = None
    time = None

    def __init__(self, data):
        data = json.loads(data)
        self.planeCode = data["planeCode"]
        self.speed = data["speed"]
        self.planeCountry = data["planeCountry"]
        self.time = data["time"]

#json_data = '{"planeCode":12345,"speed":123}'

#print json.loads(json_data)["speed"]

#event1 = FlightEvent(json_data)

#print event1.speed


vals = [1,2,3,4,5]

for x in vals:
    print x

    print "end"

print float(3) / float(2)


conf = Configuration()
print(conf.getProperty("streaming.context","duration"))


