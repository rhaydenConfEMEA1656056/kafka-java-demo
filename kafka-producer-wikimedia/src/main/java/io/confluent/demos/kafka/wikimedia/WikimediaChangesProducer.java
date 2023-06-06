package io.confluent.demos.kafka.wikimedia;


import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventSource.Builder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092"); //localhost connection

        //set Producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        Builder builder = new Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();


        //start the producer in another thread
        eventSource.start();

        //produce for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(10);


    }
}
