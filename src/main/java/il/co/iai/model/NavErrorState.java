package il.co.iai.model;

import lombok.Data;

import java.io.Serializable;
import java.util.*;

@Data
public class NavErrorState implements Serializable {

    private Map<Double, Long> nav_x_hist = new HashMap<>();
    private Map<Double, Long> nav_y_hist = new HashMap<>();
    private Map<Double, Long> nav_z_hist = new HashMap<>();

    private FlightEvent lastFlightEvent;

    private Map<Integer,Double> last5Samples = new HashMap<>(5);

    //private Double maxNavTotalErrorLast5Events;
    private Integer counter = 0;

    public void addNavErrorSample(Double navError){
        last5Samples.put(counter,navError);
        if (counter == 4)
            counter = 0;
        else
            counter++;
    }

    public Double getMaxNavTotalErrorLast5Samples(){
        //TODO do we need to return a value only if last5Samples is full?
        Double max = null;
        for (Integer i : last5Samples.keySet()) {
            if (last5Samples.get(i) != null && (max == null || max < last5Samples.get(i))) {
                max = last5Samples.get(i);
            }
        }
        return max;

    }

    public NavErrorState(){
        for (int i=0; i<5;i++){
            last5Samples.put(i,null);
        }
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
}
