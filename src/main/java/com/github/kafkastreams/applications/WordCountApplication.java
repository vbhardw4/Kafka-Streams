package com.github.kafkastreams.applications;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApplication {
    public static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static String TOPIC_TO_STREAM_FROM = "word-count-input";
    public static String TOPIC_STREAM_TO = "word-count-output";
    public static Properties config = new Properties();

    public static void main(String[] args) {
        buildStreamConfiguration();
        Topology topology = getStreamTopology();

        KafkaStreams streams = new KafkaStreams(topology,config);
        streams.start();
    }

    private static Topology getStreamTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream(TOPIC_TO_STREAM_FROM, Consumed.with(Serdes.String(),Serdes.String()))
                .mapValues(value->value.toLowerCase())
                .flatMapValues(value->Arrays.asList(value.split("\\s+")))
                .selectKey((k,v)->v)
                .groupByKey()
                .count(Named.as("Count"))
                .toStream()
                .to(TOPIC_STREAM_TO, Produced.with(Serdes.String(),Serdes.Long()));

        return streamsBuilder.build();
    }

    private static void buildStreamConfiguration() {
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    }
}
