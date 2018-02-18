from __future__ import print_function

from pyspark.streaming.kafka import KafkaDStream

from com.iai.SpeedHandler import handle as handleSpeed
from com.iai.DistanceHandler import handle as handleDistance


import json


def updateState( element, state):
    print(element)
    print(state)
    #print(state)
    return element

#class MainHandler:
 #   handlers = [handleDistance, handleSpeed]



def handle( stream = KafkaDStream):
        stream.map(lambda o: json.loads(o[1])).pprint()
           # .updateStateByKey(updateState)\

        #for handler in self.handlers:
         #   handler(stream)

        # .map(lambda o:(o["planeCode"],o)).groupByKey()\