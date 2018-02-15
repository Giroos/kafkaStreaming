package il.co.iai.model;

import lombok.*;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class FlightEvent implements Serializable {
  private String id = "";
  private EventType type;
  private Long time;
  private Long planeCode;

  private Long nav_x;
  private Long nav_y;
  private Long nav_z;

  private Long v_x;
  private Long v_y;
  private Long v_z;

  private Double nav_total_error;
  private Double nav_error_x;
  private Double nav_error_y;
  private Double nav_error_z;

  private Double nav_max_error_10sec;
  private Double nav_Mean_Error_5_samples;
  private Double nav_Error_disc;

  public static FlightEvent createFromJson(String json) {
    ObjectMapper objectMapper = new ObjectMapper();
    FlightEvent flightEvent = null;
    try {
      flightEvent = objectMapper.readValue(json, FlightEvent.class);
    } catch (IOException e) {
      flightEvent = new FlightEvent();
    }
    return flightEvent;
  }


}
