package com.github.kafkastreams.applications;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class BankingApplicationBalanceStreams {

    public  static String TOPIC_TO_READ_FROM = "banking-application-5";
    public  static  String TOPIC_TO_SEND_TO = "aggregated-banking-data";
    public  static  String APPLICATION_NAME = "bank-balance-application";
    public  static  String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    public static Properties config = new Properties();
    public static Logger logger = LoggerFactory.getLogger(BankingApplication.class.getName());

    public static void main(String[] args) {

        buildStreamsConfiguration();
        Topology topology = buildStreamsTopology();

        KafkaStreams streams = new KafkaStreams(topology,config);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Shutting down the stream gracefully....");
            streams.close();
        }));

    }

    private static Topology buildStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,Customer> bankTransactions = builder.stream(TOPIC_TO_READ_FROM,
                Consumed.with(Serdes.String(),CustomSerdes.getCustomerSerdes()))
                .peek((key,value)->{
                    logger.info("Key received is "+key);
                    logger.info("Value received is "+value);
                });

        bankTransactions.groupByKey(Grouped.with(Serdes.String(),CustomSerdes.getCustomerSerdes()))
                .aggregate(
                        ()-> new Customer(),
                        (key,transaction,balance) -> createCustomerObject(transaction,balance)
                ).toStream().to(TOPIC_TO_SEND_TO, Produced.with(Serdes.String(),CustomSerdes.getCustomerSerdes()));

        return builder.build();
    }

    private static Customer createCustomerObject(Customer transaction, Customer balance) {
        Customer customer = new Customer();
        customer.setId(transaction.getId());
        customer.setCustomerName(transaction.getCustomerName());
        customer.setAmountDeposited(transaction.getAmountDeposited()+ balance.getAmountDeposited());
        customer.setDepositedAt(transaction.getDepositedAt());

        return customer;
    }

    private static void buildStreamsConfiguration() {

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,CustomSerdes.class.getName());
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,APPLICATION_NAME);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE_V2);

    }

}
