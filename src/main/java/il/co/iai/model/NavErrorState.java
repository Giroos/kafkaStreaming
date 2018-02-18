package il.co.iai.model;

import il.co.iai.utils.StreamUtils;
import lombok.Data;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

@Data
public class NavErrorState implements Serializable {

  private List<Tuple2<Double, Long>> nav_x_hist = new ArrayList<>();
  private List<Tuple2<Double, Long>> nav_y_hist = new ArrayList<>();
  private List<Tuple2<Double, Long>> nav_z_hist = new ArrayList<>();

  private FlightEvent lastFlightEvent;
  private List<FlightEvent> handledEvents = new ArrayList<>();
  private Map<Integer, Double> last5Samples = new HashMap<>(5);

  //private Double maxNavTotalErrorLast5Events;
  private Integer counter = 0;

  public NavErrorState(FlightEvent firstEvent) {
    handle(firstEvent);
  }

  NavErrorState(NavErrorState prototype) {
    nav_x_hist = prototype.nav_x_hist;
    nav_y_hist = prototype.nav_y_hist;
    nav_z_hist = prototype.nav_z_hist;
    lastFlightEvent = prototype.lastFlightEvent;
    handledEvents = prototype.handledEvents;
    last5Samples = prototype.last5Samples;
    counter = prototype.counter;
  }

    /*public void setMaxNavTotalErrorLast5Events(Double navTotalError){
        if (maxNavTotalErrorLast5Events == null){
            maxNavTotalErrorLast5Events = navTotalError;
            counter++;
        } else {
            if (navTotalError > maxNavTotalErrorLast5Events)
                maxNavTotalErrorLast5Events = navTotalError;
            if (counter < 5)
                counter++;
        }
    }

    public Double getMaxNavTotalErrorLast5Events(){
        if (counter == 5)
            return maxNavTotalErrorLast5Events;
        return null;
    }*/

  public NavErrorState handle(FlightEvent event) {
    NavError navError = calculateNavigationError(lastFlightEvent, event);
    event.setNav_error_x(navError.getX());
    event.setNav_error_y(navError.getY());
    event.setNav_error_z(navError.getZ());
    event.setNav_total_error(navError.getTotal());

    updateHistogram(getNav_x_hist(), navError.getX());
    updateHistogram(getNav_y_hist(), navError.getY());
    updateHistogram(getNav_z_hist(), navError.getZ());
    addNavErrorSample(navError.getTotal());
    event.setNav_Mean_Error_5_samples(getMaxNavTotalErrorLast5Samples());
    handledEvents.add(event);
    return new NavErrorState(this);
  }

  public NavErrorState mergeWith(NavErrorState that) {
    NavErrorState newState = new NavErrorState(this);

    that.getNav_x_hist().forEach((navError) -> newState.updateHistogram(getNav_x_hist(), navError._1()));
    that.getNav_y_hist().forEach((navError) -> newState.updateHistogram(getNav_y_hist(), navError._1()));
    that.getNav_z_hist().forEach((navError) -> newState.updateHistogram(getNav_z_hist(), navError._1()));

    Stream<NavError> navErrors = StreamUtils.zip(
      getNav_x_hist().stream(),
      getNav_y_hist().stream(),
      getNav_z_hist().stream())
      .map(axisErrors -> {
        Double navErrorX = axisErrors._1()._1();
        Double navErrorY = axisErrors._2()._1();
        Double navErrorZ = axisErrors._3()._1();
        return new NavError(navErrorX, navErrorY, navErrorZ,
          calculateNavTotalError(navErrorX, navErrorY, navErrorZ));
      });

    that.getHandledEvents().forEach(newState.handledEvents::add);
    navErrors.map(NavError::getTotal).forEach(newState::addNavErrorSample);
    return newState;
  }

  private void addNavErrorSample(Double navError) {
    last5Samples.put(counter, navError);
    if (counter == 4)
      counter = 0;
    else
      counter++;
  }

  private Double getMaxNavTotalErrorLast5Samples() {
    //TODO do we need to return a value only if last5Samples is full?
    Double max = null;
    for (Integer i : last5Samples.keySet()) {
      if (last5Samples.get(i) != null && (max == null || max < last5Samples.get(i))) {
        max = last5Samples.get(i);
      }
    }
    return max;

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

  private void updateHistogram(List<Tuple2<Double, Long>> histogram, Double navError) {
    Optional<Integer> navErrorIdx = StreamUtils
      .indexOf(histogram, (Tuple2<Double, Long> histTuple) -> histTuple._1().equals(navError));

    if (navErrorIdx.isPresent()) {
      int idx = navErrorIdx.get();
      Tuple2<Double, Long> currentErrorCounter = histogram.get(idx);
      histogram.remove(idx);
      Tuple2<Double, Long> updatedCounter = new Tuple2<>(currentErrorCounter._1(), currentErrorCounter._2() + 1);
      histogram.add(idx, updatedCounter);
    } else {
      histogram.add(new Tuple2<>(navError, 0L));
    }
  }
}
