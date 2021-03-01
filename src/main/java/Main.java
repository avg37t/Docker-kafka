import by.epam.kafka.task2.consumer.Consumer;
import by.epam.kafka.task2.producer.Producer;
import org.slf4j.LoggerFactory;

import static by.epam.kafka.task2.constant.Constant.*;

public class Main {
    public static void main(String[] args) {



        Producer producer = new Producer();
        Consumer consumer1 = new Consumer("Consumer1", ".\\src\\main\\java\\first_consumer.txt");
        Consumer consumer2 = new Consumer("Consumer2", ".\\src\\main\\java\\second_consumer.txt");

        try {
            producer.start();
            consumer1.start();
            Thread.sleep(20000);
            consumer2.start();
        } catch (InterruptedException e) {
            LoggerFactory.getLogger(Main.class).error(INTERRUPT, e);
        }

    }
}
