package il.co.iai.etl;

import il.co.iai.model.FeaturesCache;
import il.co.iai.model.FlightEvent;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class MainHandler implements EventHandler {

    @Autowired
    private List<EventHandler> handlers;


    public JavaDStream<FlightEvent> handle(JavaDStream<FlightEvent> dStream) {
        for (EventHandler handler : handlers) {
            handler.handle(dStream);
        }
        return null;
    }

    public void testHandle(JavaPairDStream<String, String> dStream){
        dStream.print();
    }




}
