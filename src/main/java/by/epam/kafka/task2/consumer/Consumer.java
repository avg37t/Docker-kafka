package by.epam.kafka.task2.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static by.epam.kafka.task2.constant.Constant.*;

public class Consumer extends Thread {

    private String consumerName;
    private String filePath;

    public Consumer(String name, String filePath) {
        this.consumerName = name;
        this.filePath = filePath;
    }

    @Override
    public void run() {
        final Logger logger = LoggerFactory.getLogger(Consumer.class);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_VALUE);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, SEQUENCE);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton(TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info(record.value());
                try (FileWriter writer = new FileWriter(filePath, true)) {
                //    System.out.println(consumerName + " " + record.value() + " " + "Partition:" + record.partition() + filePath + "\n");
                    writer.write(consumerName + " " + record.value() + " " + "Partition:" + record.partition() + "\n");
                    writer.flush();
                } catch (IOException e) {
                    logger.error(FILE_ERROR, e);
                }
            }
        }
    }
}

