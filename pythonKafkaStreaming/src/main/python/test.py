from __future__ import print_function

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import json


def handleSpeed(event):
    print (str(event["planeCode"]) + " HAD SPEED " + str(event["speed"]))


def handleAcceleration(tup):
    prevSpeed = 0
    prevTime = 0
    for x in tup[1]:
        if (prevTime == 0):
            prevTime = x["time"]
            prevSpeed = x["speed"]
        elif  (x["speed"] - prevSpeed >= 100 & x["time"] - prevSpeed<=1000):
            print("PLANE " + str(tup[0]) + " HAD FAST ACCELERATION")


def handleAverageSpeed(tup):
    speedSum = 0
    count = 0
    for x in tup[1]:
        speedSum = speedSum + x["speed"]
        count = count + 1
    print (str(tup[0]) + " HAS AVERAGE SPEED " + str(float(speedSum) / float(count)))



sc = SparkContext("local[2]", "NetworkWordCount")
#,pyFiles=['file:/D:/opt/spark-streaming-kafka-0-8-assembly_2.10-2.2.0.jar'])
ssc = StreamingContext(sc, 5)

kafkaReceiverParams = {
    "metadata.broker.list": "localhost:9092",
    "auto.offset.reset": "largest",
    "group.id": "gena",
    "client.id":"spark-streaming-consumer"}

kafkaStream = KafkaUtils.createDirectStream(ssc, ["flightEvents2"], kafkaReceiverParams)

countries = [11,12,13]

kafkaStream.pprint()

filtered = kafkaStream.map(lambda o: json.loads(o[1])).filter(lambda e: countries.__contains__(e["planeCountry"]))

filtered.filter(lambda e: e["speed"] > 2000).foreachRDD(lambda rdd: rdd.foreach(lambda e:handleSpeed(e)))

filtered.window(50).map(lambda e: (e["planeCode"], e)).groupByKey(1).map(lambda x : (x[0], list(x[1]))) \
    .foreachRDD(lambda o:o.foreach(
    lambda e:handleAcceleration(e)))

filtered.window(200).map(lambda e: (e["planeCode"], e)).groupByKey(1).map(lambda x : (x[0], list(x[1]))) \
    .foreachRDD(lambda o:o.foreach(
    lambda e:handleAverageSpeed(e)))


ssc.start()
ssc.awaitTermination()


#{"time":123456,"speed":10000, "planeCode":333, "planeCountry":11}