from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from com.iai.MainHandler import handle
from com.iai.Configuration import Configuration



conf = Configuration()

sc = SparkContext(conf.getProperty("spark","master"), conf.getProperty("spark","app.name"))

ssc = StreamingContext(sc, int(conf.getProperty("streaming.context","duration")))
kafkaReceiverParams = {
    "metadata.broker.list": conf.getProperty("streaming.context","metadata.broker.list"),
    "auto.offset.reset": conf.getProperty("streaming.context","auto.offset.reset"),
    "group.id": conf.getProperty("streaming.context","group.id"),
    "client.id": conf.getProperty("streaming.context","client.id")}

ssc.checkpoint(conf.getProperty("checkpoint.dir"))

kafkaStream = KafkaUtils.createDirectStream(ssc, [conf.getProperty("streaming.context","topic")], kafkaReceiverParams)


#mainHandler = MainHandler()
#mainHandler.handle(kafkaStream)
handle(kafkaStream)

ssc.start()
ssc.awaitTermination()
