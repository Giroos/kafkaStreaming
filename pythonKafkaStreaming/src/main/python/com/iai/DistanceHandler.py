from pyspark.streaming.kafka import KafkaDStream

def handle( stream = KafkaDStream):
    stream.pprint()