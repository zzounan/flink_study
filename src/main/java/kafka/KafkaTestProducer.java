package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class KafkaTestProducer {

    public static void main(String[] args) throws InterruptedException {

        // Kafka 配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String[] names = {"Alice", "Bob", "Charlie", "David", "Eva"};
        Random random = new Random();

        System.out.println("Starting to send test data to Kafka topic 'test-topic'...");

        while (true) {
            String name = names[random.nextInt(names.length)];
            int age = random.nextInt(50) + 10; // 10~59
            String message = name + "," + age;

            // 发送消息
            producer.send(new ProducerRecord<>("test-topic", message));

            System.out.println("Sent: " + message);

            Thread.sleep(1000); // 每秒发送一条
        }
    }
}