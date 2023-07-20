package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConstants.producerClientId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.bootStrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        produceKStream(props); // the topic is consumed thru streams
//        produceKStreamToKTableToKStream(props); // the topic is consumed thru streams -> stored in KTable -> produced to topic that is consumed thru KStream
//        produceToTwoTopicsForJoin(props); //
        produceToRegularTopic(props);
    }

    private static void produceToRegularTopic(Properties props) {
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        for (int j = 0; j <= 5; j++) {
            for (int i = 0; i <= 9; i++) {
                producer.send(new ProducerRecord<>(AppConstants.regularTopic, i, "this is a new message _" + i + "" + j));
                System.out.println(i + " -> this is a new message _" + i + "" + j);
            }
        }
        producer.close();
    }

    private static void produceToTwoTopicsForJoin(Properties props) {
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        for (int i = 0; i <= 9; i++) {
            producer.send(new ProducerRecord<>(AppConstants.JoinATopic, i, "JoinA_" + i));
            System.out.println(i + "JoinA_" + i);
            producer.send(new ProducerRecord<>(AppConstants.JoinBTopic, i, "_JoinB_" + i));
            System.out.println(i + "_JoinB" + i);
            producer.send(new ProducerRecord<>(AppConstants.JoinKTableTopic, i,"_#" + "BothJoined_" + i));
        }
        producer.close();
    }

    private static void produceKStream(Properties props) {
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        for (int j = 0; j <= 5; j++) {
            for (int i = 0; i <= 9; i++) {
                producer.send(new ProducerRecord<>(AppConstants.kStreamTopic, i, "this is a new message _" + i + "" + j));
                System.out.println(i + " -> this is a new message _" + i + "" + j);
            }
        }
        producer.close();
    }

    private static void produceKStreamToKTableToKStream(Properties props) {
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        for (int j = 0; j <= 5; j++) {
            for (int i = 0; i <= 9; i++) {
                producer.send(new ProducerRecord<>(AppConstants.kTableTopic, i, "this is a new message _" + i + "" + j));
                System.out.println(i + " -> this is a new message _" + i + "" + j);
            }
        }
        producer.close();
    }
}
