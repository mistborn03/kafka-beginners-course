package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    String bootstrapServers = "127.0.0.1:9092";
    Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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

      String topic = "second_topic";
      String key = "id_" + Integer.toString(i);
      String value = "hello world " + Integer.toString(i);

      logger.info("Key: " + key);

      // create producer record
      ProducerRecord<String, String> record = new ProducerRecord<>(topic,key, value);

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
          }).get(); // to block send() ; not to be done in production code, just for experimentation
    }

    // flush data
    producer.flush();

    // flush and close producer
    producer.close();
  }
}
