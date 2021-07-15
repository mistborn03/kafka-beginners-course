package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

  public static void main(String[] args) {
    new ConsumerDemoWithThread().run();
  }

  private void run() {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "my-fourth-application";
    String topic = "second_topic";

    //latch to deal with multiple threads
    CountDownLatch latch = new CountDownLatch(1);

    //create consumer runnable
    logger.info("Creating the consumer thread");
    Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

    //start thread
    Thread myThread = new Thread(myConsumerRunnable);
    myThread.start();

    //add a shutdown hook
      Runtime.getRuntime().addShutdownHook(new Thread(()->{
          logger.info("caught shutdown hook");
          ((ConsumerRunnable) myConsumerRunnable).shutdown();
        try {
          latch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        logger.info("application has exited");
      }));

    // wait till application is over
      try {
          latch.await();
      } catch (InterruptedException e) {
          logger.error("Application got interrupted",e);
      }finally{
          logger.info("Application is closing");
      }
  }

  public ConsumerDemoWithThread() {}

  public class ConsumerRunnable implements Runnable {

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;
    private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    public ConsumerRunnable(
        String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
      this.latch = latch;

      // create consumer configs
      Properties properties = new Properties();
      properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      // create consumer
      consumer = new KafkaConsumer<String, String>(properties);

      // subscribe a consumer to topic(s)
      consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
      try {
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

          for (ConsumerRecord<String, String> record : records) {
            logger.info("Key : " + record.key() + " value: " + record.value());
            logger.info("partition: " + record.partition() + " offset " + record.offset());
          }
        }
      } catch (WakeupException e) {
        logger.info("received shutdown signal!");
      } finally {
        consumer.close();
        // tell main code we're done with consumer
        latch.countDown();
      }
    }

    public void shutdown() {
      // wakeup() method  is a special method to interrupt consumer.poll()
      // will throw wakeup exception
      consumer.wakeup();
    }
  }
}
