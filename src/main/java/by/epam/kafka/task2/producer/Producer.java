package by.epam.kafka.task2.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static by.epam.kafka.task2.constant.Constant.*;

public class Producer extends Thread {

    @Override
    public void run() {

        final Logger logger = LoggerFactory.getLogger(Producer.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_VALUE);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 1; i <= 1000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, MESSAGE + i);
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info(HEADER +
                            TOPIC_STR + recordMetadata.topic() + "\n" +
                            PARTITION + recordMetadata.partition() + "\n" +
                            OFFSET + recordMetadata.offset() + "\n" +
                            TIMESTAMP + recordMetadata.timestamp());
                } else logger.error(ERR_PR, e);
            });
            producer.flush();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.error(INTERRUPT,e);
            }
        }
        producer.close();
    }
}
