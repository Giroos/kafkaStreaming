from __future__ import print_function

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import com.iai.MainHandler

import json


def createContext():
    sc = SparkContext("local[2]", "FlightEvents")
    # ,pyFiles=['file:/D:/opt/spark-streaming-kafka-0-8-assembly_2.10-2.2.0.jar'])
    ssc = StreamingContext(sc, 3)
    kafkaReceiverParams = {
        "metadata.broker.list": "localhost:9092",
        "auto.offset.reset": "largest",
        "group.id": "gena",
        "client.id": "spark-streaming-consumer"}
    ssc.checkpoint("D:\\tmp\\checkpoint")

    kafkaStream = KafkaUtils.createDirectStream(ssc, ["flightEvents2"], kafkaReceiverParams)
    kafkaStream.checkpoint(3)

    mainHandler = MainHandler()
    mainHandler.handle(kafkaStream)
    return ssc


ssc = StreamingContext.getOrCreate("D:\\tmp\\checkpoint", createContext)

ssc.start()
ssc.awaitTermination()



#SPARK_CLASSPATH was detected (set to 'D:\opt\spark-streaming-kafka-0-8-assembly_2.11-2.2.1.jar').