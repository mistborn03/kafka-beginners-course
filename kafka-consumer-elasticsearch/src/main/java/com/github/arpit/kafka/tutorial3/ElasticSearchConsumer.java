package com.github.arpit.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

  public static RestHighLevelClient createClient() {

    String hostname = "kafka-course-5231313789.eu-west-1.bonsaisearch.net";
    String username = "4txy0g2aht";
    String password = "s60bgco06q";

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        AuthScope.ANY, new UsernamePasswordCredentials(username, password));

    RestClientBuilder builder =
        RestClient.builder(new HttpHost(hostname, 443, "https"))
            .setHttpClientConfigCallback(
                new RestClientBuilder.HttpClientConfigCallback() {
                  @Override
                  public HttpAsyncClientBuilder customizeHttpClient(
                      HttpAsyncClientBuilder httpAsyncClientBuilder) {
                    return httpAsyncClientBuilder.setDefaultCredentialsProvider(
                        credentialsProvider);
                  }
                });

    RestHighLevelClient client = new RestHighLevelClient(builder);
    return client;
  }

  public static KafkaConsumer<String, String> createConsumer(String topic) {
    String bootstrapServers = "127.0.0.1:9092";
    Properties properties = new Properties();
    String groupId = "kafka-demo-elasticsearch";

    // create consumer configs
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

    // create a consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    // subscribe a consumer to topic(s)
    consumer.subscribe(Arrays.asList(topic));

    return consumer;
  }

  public static void main(String[] args) throws Exception {

    Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    RestHighLevelClient client = createClient();

    KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      Integer recordCount = records.count();
      logger.info("Received " + recordCount + " records");

      BulkRequest bulkRequest = new BulkRequest();

      for (ConsumerRecord<String, String> record : records) {
        try {
          String id = extractIdFromTweet(record.value());
          // insert data to elasticsearch
          IndexRequest indexRequest =
              new IndexRequest(
                      "twitter", "tweets", id // to make consumer idempotent
                      )
                  .source(record.value(), XContentType.JSON);

          bulkRequest.add(indexRequest);
        } catch (NullPointerException e) {
          logger.warn("skipping bad data:" + record.value());
        }
      }
      if (recordCount > 0) {
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        logger.info("Committing offsets");
        consumer.commitSync();
        logger.info("Offsets committed");
        Thread.sleep(1000);
      }
    }

    // close client
    // client.close();
  }

  private static JsonParser jsonParser;

  private static String extractIdFromTweet(String tweetJson) {
    return jsonParser.parseString(tweetJson).getAsJsonObject().get("id_str").getAsString();
  }
}
