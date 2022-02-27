package com.github.kafkastreams.applications;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CustomSerializer;
import utils.CustomerSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;


public class BankingApplication {

    public static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static String TOPIC_STREAM_TO = "banking-application-5";

    public static Properties config = new Properties();
    public static Logger logger = LoggerFactory.getLogger(BankingApplication.class.getName());

    public static ObjectMapper objectMapper = new ObjectMapper();
    public static SimpleModule simpleModule = new SimpleModule();

    public static void main(String[] args) {

        simpleModule.addSerializer(Customer.class,new CustomerSerializer());
        objectMapper.registerModule(simpleModule);

        buildProducerConfiguration();
        KafkaProducer<String,Customer> producer = new KafkaProducer<String, Customer>(config);

        // Code for gracefully shutting down the application

        try {
            int i=0;
            while (true) {
                producer.send(generateProducerRecords("Bob"));
                Thread.sleep(100);
                producer.send(generateProducerRecords("Marley"));
                Thread.sleep(100);
                producer.send(generateProducerRecords("Jenna"));
            }
        }
        catch (Exception e) {
            logger.error("Exception while pushing data to the Kafka "+e.getMessage());
            producer.close();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Shutting down the application gracefully");
            producer.close();
            logger.info("Application graceful shutdown successful");

        }));
    }

    private static ProducerRecord<String, Customer> generateProducerRecords(String name) {
        logger.info("Generating producer record for customer "+name);
        String generatedJSON = null;
        Customer customer = new Customer();

        Instant now = Instant.now();
        int amount = new Random().nextInt(500);

        customer.setCustomerName(name);
        customer.setAmountDeposited(amount);
        customer.setDepositedAt(now.toString());

        try {
            generatedJSON = objectMapper.writeValueAsString(customer);
            logger.info("Record generated for customer "+name+" and JSON is "+
                    generatedJSON);
        } catch (JsonProcessingException e) {
            logger.error("Error while converting JSON to Customer Object");
            e.printStackTrace();
        }

        return new ProducerRecord<String,Customer>(TOPIC_STREAM_TO,name,customer);
    }

    private static void buildProducerConfiguration() {
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class.getName());
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        // Making the producer idempotent
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true"); // not pushing duplicating data more than once
        config.put(ProducerConfig.ACKS_CONFIG,"all"); // Data has been replicated across all in-sync replicas
        config.put(ProducerConfig.LINGER_MS_CONFIG,"1"); // Sending data every 1ms
        config.put(ProducerConfig.RETRIES_CONFIG,"3");
    }
}
