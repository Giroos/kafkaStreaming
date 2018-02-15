package il.co.iai.etl;

import il.co.iai.model.FlightEvent;
import il.co.iai.model.NavError;
import il.co.iai.model.NavErrorState;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static il.co.iai.model.EventType.NAV;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

@Component
public class NavigationErrorEventHandler implements EventHandler {

    @Autowired
    private Long systemStartTime;

    @Override
    public JavaDStream<FlightEvent> handle(JavaDStream<FlightEvent> dStream) {
        JavaMapWithStateDStream<Long, Iterable<FlightEvent>, NavErrorState, Iterable<FlightEvent>> mapped =
                dStream.filter(e -> NAV.equals(e.getType()))
                        .transformToPair(rdd -> rdd.sortBy(FlightEvent::getTime, true, 1).groupBy(FlightEvent::getPlaneCode))
                        .mapWithState(StateSpec.function((Long key, Optional<Iterable<FlightEvent>> events, State<NavErrorState> state) -> {

                            FlightEvent last = null;
                            NavErrorState navErrorState = null;
                            if (state.exists()) {
                                navErrorState = state.get();
                                last = navErrorState.getLastFlightEvent();
                            } else {
                                navErrorState = new NavErrorState();
                            }

                            for (FlightEvent event : events.get()) {
                                if (last != null) {
                                    NavError navError = calculateNavigationError(last, event);
                                    event.setNav_error_x(navError.getX());
                                    event.setNav_error_y(navError.getY());
                                    event.setNav_error_z(navError.getZ());
                                    event.setNav_total_error(navError.getTotal());

                                    updateHistogram(navErrorState.getNav_x_hist(), navError.getX());
                                    updateHistogram(navErrorState.getNav_y_hist(), navError.getY());
                                    updateHistogram(navErrorState.getNav_z_hist(), navError.getZ());

                                    navErrorState.addNavErrorSample(navError.getTotal());

                                    event.setNav_Mean_Error_5_samples(navErrorState.getMaxNavTotalErrorLast5Samples());
                                }
                                last = event;
                            }
                            navErrorState.setLastFlightEvent(last);
                            state.update(navErrorState);
                            return events.get();
                        }));



        mapped.window(Duration.apply(60000))
                .mapToPair(iter -> Tuple2.apply(iter.iterator().next().getPlaneCode(), iter))
                .reduceByKey((flightEvents, flightEvents2) -> {
                    List<FlightEvent> result = new ArrayList();
                    flightEvents.forEach(result::add);
                    flightEvents2.forEach(result::add);
                    return result;
                })
                .mapToPair(pair -> {
                    Long currentTime = System.currentTimeMillis();
                    if (currentTime - systemStartTime >= 30000) {
                        Double max = null;
                        for (FlightEvent flightEvent : pair._2) {
                            if (max == null || max < flightEvent.getNav_total_error())
                                max = flightEvent.getNav_total_error();
                        }
                        for (FlightEvent flightEvent : pair._2) {
                            flightEvent.setNav_max_error_10sec(max);
                        }
                    }
                    //Long plane = iter.iterator().hasNext() ? iter.iterator().next().getPlaneCode() : null;
                    return Tuple2.apply(pair._1, pair._2);
                }).print();
        //mapped.print();
        mapped.stateSnapshots().print();

        return null;
    }

    private NavError calculateNavigationError(FlightEvent prev, FlightEvent current) {
        Long t = current.getTime() - prev.getTime();

        Double navErrorX = calculateForAxis(current.getNav_x() - prev.getNav_x(), current.getV_x() + prev.getV_x(), t);
        Double navErrorY = calculateForAxis(current.getNav_y() - prev.getNav_y(), current.getV_y() + prev.getV_y(), t);
        Double navErrorZ = calculateForAxis(current.getNav_z() - prev.getNav_z(), current.getV_z() + prev.getV_z(), t);
        Double totalError = calculateNavTotalError(navErrorX, navErrorY, navErrorZ);

        return new NavError(navErrorX, navErrorY, navErrorZ, totalError);
    }

    private Double calculateForAxis(Long axis, Long v, Long t) {
        return (double) axis - ((double) t * (double) v / 2);
    }

    private Double calculateNavTotalError(Double navErrorX, Double navErrorY, Double navErrorZ) {

        return sqrt(pow(navErrorX, 2) + pow(navErrorY, 2) + pow(navErrorZ, 2));

    }

    private void updateHistogram(Map<Double, Long> histMap, Double navError) {
        if (histMap.containsKey(navError)) {
            histMap.put(navError, histMap.get(navError) + 1);
        } else {
            histMap.put(navError, 1L);
        }
    }
}
