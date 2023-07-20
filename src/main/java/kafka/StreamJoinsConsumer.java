package kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class StreamJoinsConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConstants.KStreamJoinGroup);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.bootStrapServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1); // can increase the number of threads

        final Serde<Integer> integerSerde = Serdes.Integer();
        final Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, String> kStreamA = builder.stream(
                AppConstants.JoinATopic,
                Consumed.with(integerSerde, stringSerde)
        );
        KStream<Integer, String> kStreamB = builder.stream(
                AppConstants.JoinBTopic,
                Consumed.with(integerSerde, stringSerde)
        );
        KTable<Integer, String> kTable = builder.table(AppConstants.JoinKTableTopic, Consumed.with(Serdes.Integer(), Serdes.String()));

        ValueJoiner<String, String, String> kStreamJoiner = (s1, s2) -> s1 + s2;
        ValueJoiner<String, String, String> kTableJoiner = (s1, s2) -> s1 + s2;

        KStream<Integer, String> kStreamJoined = kStreamA.join(kStreamB, kStreamJoiner, JoinWindows.of(Duration.ofMillis(1)),
                StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String()))
                .peek((k, v) -> System.out.println("key: " + k + " value: " + v + " thread:" + Thread.currentThread().getId()));


        kStreamJoined.leftJoin(kTable, kTableJoiner, Joined.with(Serdes.Integer(), Serdes.String(), Serdes.String()))
                .peek((k, v) -> System.out.println("key: " + k + " value: " + v + " thread:" + Thread.currentThread().getId()))
                        .to(AppConstants.JoinOutputKTableTopic, Produced.with(Serdes.Integer(), Serdes.String()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
        }));
    }
}
