package il.co.iai;

import il.co.iai.etl.MainHandler;
import il.co.iai.model.FlightEvent;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.*;

@Configuration
@ComponentScan
@PropertySource("classpath:application.properties")
public class Config {

  @Value("${spark.master}")
  private String master;

  @Value("${spark.app.name}")
  private String appName;

  @Bean
  public SparkConf sparkConf(){
    SparkConf conf = new SparkConf().setMaster(master).setAppName(appName);
    return conf;
  }


  @Value("${streaming.context.metadata.broker.list}")
  private String brokers;

  @Value("${streaming.context.client.id}")
  private String clientId;

  @Value("${streaming.context.group.id}")
  private String groupId;

  @Value("${streaming.context.auto.offset.reset}")
  private String offset;

  @Bean
  public Map<String, String> kafkaParams(){
    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", brokers);
    kafkaParams.put("client.id", clientId);
    kafkaParams.put("group.id", groupId);
    kafkaParams.put("auto.offset.reset", offset);

    return kafkaParams;
  }



   /* @Bean
    public JavaStreamingContext javaStreamingContext(){
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf(), Durations.milliseconds(duration));
        jssc.checkpoint(checkpointDir);

        return jssc;

    }

    private JavaStreamingContext createStreamingContext(){
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf(), Durations.milliseconds(duration));
        jssc.checkpoint(checkpointDir);

        return jssc;
    }*/

  @Value("${streaming.context.topic}")
  private String topic;

  @Bean
  public Set<String> topics(){
    Set<String> topics = new HashSet<>();
    topics.add(topic);
    return topics;
  }



  @Value("${streaming.context.duration}")
  private long duration;

  @Value("${streaming.context.checkpoint.dir}")
  private String checkpointDir;

  @Autowired
  private MainHandler mainHandler;

  @Value("#{'${flight.countries}'.split(',')}")
  private List<Long> countriesList;

  public JavaStreamingContext createStreamingContext(){
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf(), Durations.milliseconds(duration));
    jssc.checkpoint(checkpointDir);

    JavaPairInputDStream<String, String> streamNoOffset = KafkaUtils.createDirectStream(
            jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams(), topics());
    streamNoOffset.checkpoint(Duration.apply(duration));
    //streamNoOffset.print();




    JavaDStream<FlightEvent> filteredAndConverted = streamNoOffset.map(tuple -> tuple._2).map(FlightEvent::createFromJson);

    //mainHandler.testHandle(streamNoOffset.filter(tuple -> !tuple._2.equalsIgnoreCase("123")));

    mainHandler.handle(filteredAndConverted);
    return jssc;
  }

  @Bean
  public Long systemStartTime(){
    return System.currentTimeMillis();
  }

  @Value("${streaming.create.mode}")
  private String streamingCreateMode;

  private final String CREATE_MODE_NEW = "NEW";
  private final String CREATE_MODE_FROM_CHECKPOINT = "FROM_CHECKPOINT";

  @Bean
  public JavaStreamingContext javaStreamingContext() throws InterruptedException {
    if (CREATE_MODE_FROM_CHECKPOINT.equals(streamingCreateMode))
      return JavaStreamingContext.getOrCreate(checkpointDir, this::createStreamingContext);
    else
      return createStreamingContext();
  }
}
