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

import static il.co.iai.model.EventType.NAV;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

@Component
public class NavigationErrorEventHandler implements EventHandler {

  @Autowired
  private Long systemStartTime;

  @Override
  public JavaDStream<FlightEvent> handle(JavaDStream<FlightEvent> dStream) {
    return dStream
      .filter(e -> NAV.equals(e.getType()))
      .transformToPair(rdd -> rdd
        .sortBy(FlightEvent::getTime, true, 1)
        .mapToPair(event -> new Tuple2<>(event.getPlaneCode(), event)))
      .combineByKey(
        NavErrorState::new,
        NavErrorState::handle,
        NavErrorState::mergeWith,
        new HashPartitioner(8)
      )
      .mapWithState(StateSpec.function((Long key, Optional<NavErrorState> currentNavError, State<NavErrorState> state) -> {
          NavErrorState updatedState;
          if (currentNavError.isPresent()) {
            updatedState = currentNavError.get().mergeWith(state.get());
          } else {
            updatedState = state.get();
          }
          return updatedState;
        }
      ))
      .stateSnapshots()
      .window(Duration.apply(60000))
      .flatMap(navErrorState -> {
        Long currentTime = System.currentTimeMillis();
        List<FlightEvent> handledEvents = navErrorState._2.getHandledEvents();
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
