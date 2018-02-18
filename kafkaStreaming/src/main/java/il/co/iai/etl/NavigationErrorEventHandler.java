package il.co.iai.etl;

import il.co.iai.model.FlightEvent;
import il.co.iai.model.NavError;
import il.co.iai.model.NavErrorState;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static il.co.iai.model.EventType.NAV;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

@Component
public class NavigationErrorEventHandler implements EventHandler {

  @Autowired
  private Long systemStartTime;

  @Override
  public JavaDStream<FlightEvent> handle(JavaDStream<FlightEvent> dStream) {
    JavaPairDStream<Long, NavErrorState> navErrorsStream = dStream
      .filter(e -> NAV.equals(e.getType()))
      .transformToPair(rdd -> rdd
        .sortBy(FlightEvent::getTime, true, 1)
        .mapToPair(event -> new Tuple2<>(event.getPlaneCode(), event)))
      .combineByKey(
        NavErrorState::new,
        NavErrorState::handle,
        NavErrorState::mergeWith,
        new HashPartitioner(8)
      );

    return navErrorsStream
      .updateStateByKey((navErrorStates, partitioner) -> navErrorStates.stream()
        .reduce(NavErrorState::mergeWith)
        .map(state -> Optional.of((Object) state))
        .orElse(Optional.empty())
      )
      .window(Duration.apply(60000))
      .flatMap(tuple -> {
        NavErrorState navErrorState = (NavErrorState) tuple._2();
        Long currentTime = System.currentTimeMillis();
        List<FlightEvent> handledEvents = navErrorState.getHandledEvents();
        if (currentTime - systemStartTime >= 30000) {
          Double max = handledEvents.stream().max(Comparator.comparing(FlightEvent::getNav_total_error))
            .map(FlightEvent::getNav_total_error)
            .orElse(0.0);

          for (FlightEvent flightEvent : handledEvents) {
            flightEvent.setNav_max_error_10sec(max);
          }
        }
        return handledEvents.iterator();
      });
  }
}
