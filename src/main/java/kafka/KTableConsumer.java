package kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

public class KTableConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConstants.kTableGroup);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.bootStrapServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1); // can increase the number of threads


        StreamsBuilder builder = new StreamsBuilder();
        KTable<Integer, String> kTable = builder.table(
                AppConstants.kStreamTopic,
                Materialized.<Integer, String, KeyValueStore<Bytes, byte[]>>as("ktable-store")
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(Serdes.String()));

        kTable.filter((k, v) -> v.contains("this is a new message _"))
                .mapValues(v -> v.substring(v.indexOf("_")+1))
                //.filter((k, v) -> Long.parseLong(v))
                .toStream()
                .peek((k, v) -> System.out.println("key: " + k + " value: " + v + " thread:" + Thread.currentThread().getId()))
                .to(AppConstants.kTableTopic, Produced.with(Serdes.Integer(), Serdes.String()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
        }));
    }
}
