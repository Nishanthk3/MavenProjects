package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;


public class Consumer {

    public static void main(String args[]) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.bootStrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, AppConstants.regularTopicGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Kafka maintains the consumer's offset on the server side.
        // The auto config set to true will cause automatic syncs to Kafka to mark the latest offset.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.TRUE);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);

        //This is the configurated request timeout from the broker side.
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 700000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        //The consumer must call poll within this timeout period.
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 180000);

        // This property selects where to start reading from in case the consumer info is not available from Kafka.
        // Selecting "earliest" will cause consumer to read from the start.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(props);
        Runnable runnable = () -> {
            try {
                kafkaConsumer.subscribe(Collections.singleton(AppConstants.regularTopic));
                System.out.println("initial started");
                while (true) {
                    System.out.println("started");
                    ConsumerRecords<Integer, String> consumerRecords = kafkaConsumer.poll(5000);
                    consumerRecords.forEach(i -> {
                        System.out.println(i.toString());
                        TopicPartition topicPartition = new TopicPartition(i.topic(), i.partition());
                        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(i.offset());
                        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                        map.put(topicPartition, offsetAndMetadata);
                      //  kafkaConsumer.commitSync(map);
                    });
                }
            } catch (WakeupException ex) {
                System.out.println("Shutting down consumer gracefully");
            } catch (Exception ex) {
                System.out.println("Unexpected Exception:" + ex);
            } finally {
                System.out.println("Closing the consumer gracefully");
                kafkaConsumer.close();
            }
        };

        Thread thread = new Thread(runnable);
        //thread.setDaemon(true);
        thread.start();

        final Thread runnableThread = thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Waking up the consumer");
                kafkaConsumer.wakeup();
                try {
                    runnableThread.join(); // this
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }

            }
        });

    }
}
