package il.co.iai.etl;

import il.co.iai.model.FlightEvent;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;

public interface EventHandler extends Serializable {
  JavaDStream<FlightEvent> handle(JavaDStream<FlightEvent> dStream);
}
