package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {

  public static void main(String[] args) {
    String bootstrapServers = "127.0.0.1:9092";
    Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);

    // create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    for (int i = 0; i < 10; i++) {

      // create producer record
      ProducerRecord<String, String> record =
          new ProducerRecord<>("first_topic", "hello world" + Integer.toString(i));

      // send data
      producer.send(
          record,
          new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              if (e == null) {
                logger.info(
                    "\n"
                        + "Topic: "
                        + recordMetadata.topic()
                        + "\n"
                        + "Partition: "
                        + recordMetadata.partition()
                        + "\n"
                        + "timestamp: "
                        + recordMetadata.timestamp()
                        + "\n"
                        + "Offset: "
                        + recordMetadata.offset());
              } else {
                logger.error("error while producing", e);
              }
            }
          });
    }

    // flush data
    producer.flush();

    // flush and close producer
    producer.close();
  }
}
