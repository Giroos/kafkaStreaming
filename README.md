# gena-etl

## Spring config and EventHandler
[EventHandler](src/main/java/il/co/iai/etl/EventHandler.java) is the main interface having method
'handle' taking events stream as a parameter and returning a stream where each event has some features added.
```java
public interface EventHandler extends Serializable {
  JavaDStream<FlightEvent> handle(JavaDStream<FlightEvent> dStream);
}
```

'EventHandler's could be chained in this way:
```java
    EventHandler[] handlers = new EventHandler[]{distanceHandler};

    JavaDStream<FlightEvent> result = filteredAndConverted;
    for (EventHandler handler: handlers) {
      result = handler.handle(result);
    }
```

## Distance handler
[Distance handler](src/main/java/il/co/iai/etl/DistanceBetweenPlanesEventHandler.java) searches the nearest plane to the given event in the specified time window.
```java
@Override
  public JavaDStream<FlightEvent> handle(JavaDStream<FlightEvent> dStream) {
    return dStream.transform(rdd -> rdd
      .sortBy(FlightEvent::getTime, false, 1))
      .transform(eventsRdd -> {
        JavaRDD<FlightEvent> eventWithNearestPlaneRDD = eventsRdd.cartesian(eventsRdd)
          .mapToPair(eventsTuple -> {
            FlightEvent currentEvent = eventsTuple._1;
            FlightEvent event = eventsTuple._2;
            return new Tuple2<>(currentEvent.getId(), new Tuple3<>(currentEvent, event, getDistance(currentEvent, event)));
          })
          .filter(this::isNotIdentityPair)
          .filter(tuple -> isInTimeWindow(tuple._2._1(), tuple._2._2()))
          .reduceByKey((tuple1, tuple2) -> {
            long distance1 = tuple1._3();
            long distance2 = tuple2._3();
            if (distance1 < distance2) return tuple1;
            else return tuple2;
          })
          .map(tuple -> {
            FlightEvent currentEvent = tuple._2._1();
            FlightEvent nearestEvent = tuple._2._2();
            return currentEvent.addFeature(FeatureKeys.NEAREST_PLANE, nearestEvent.getPlaneCode());
          });

        return eventWithNearestPlaneRDD;
      });
  }
```