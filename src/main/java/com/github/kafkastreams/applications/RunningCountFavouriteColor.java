package com.github.kafkastreams.applications;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class RunningCountFavouriteColor {

    public static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static String TOPIC_TO_STREAM_FROM = "user-favourite-color-input";
    public static String TOPIC_STREAM_TO = "favourite--color-count-output";
    public static String APPLICATION_ID = "running-count-favourite-color";
    public static Logger logger = LoggerFactory.getLogger(RunningCountFavouriteColor.class.getName());

    public static Properties config = new Properties();
    public static KafkaStreams streams = null;

    public static void main(String[] args) {
        gracefulShutdown();
        buildStreamConfiguration();
        Topology topology = getStreamTopology();

        streams = new KafkaStreams(topology,config);
        streams.cleanUp();
        streams.start();
    }

    private static void gracefulShutdown() {
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Shutting down the application...");
            streams.close();
        }));
    }

    private static void buildStreamConfiguration() {
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private static Topology getStreamTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> usersAndColors = builder.stream(TOPIC_TO_STREAM_FROM, Consumed.with(Serdes.String(),Serdes.String()))
                .filter((k,v)->v.contains(","))
                .selectKey((k,v)->v.split(",")[0].toLowerCase())
                .mapValues(value->value.split(",")[1].toUpperCase())
                .filter((k,v)->isValidColor(v));

        usersAndColors.to("user-keys-and-colors");

        KTable<String,String> userAndColorsTable = builder.table("user-keys-and-colors");

        KTable<String,Long> favouriteColorCount =  userAndColorsTable.groupBy((user,color)->new KeyValue<>(color,color))
                .count(Named.as("CountByColors"));

        favouriteColorCount.toStream().to(TOPIC_STREAM_TO, Produced.with(Serdes.String(),Serdes.Long()));
        return builder.build();

    }

    private static boolean isValidColor(String color) {
        return Arrays.asList(Colors.values()).contains(color);
    }

}

enum Colors {
    RED,
    BLUE,
    GREEN;
}
