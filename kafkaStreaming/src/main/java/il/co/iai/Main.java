package il.co.iai;


import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;


public class Main {

    public static void main(String[] args) throws InterruptedException {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config.class);
        JavaStreamingContext jssc = context.getBean(JavaStreamingContext.class);

        jssc.start();
        jssc.awaitTermination();
    }
}
