package flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Properties;

public class KafkaReadDemo {

    public static void main(String[] args) throws Exception {

        // 1. 创建本地流执行环境，避免 ExecutorFactory 错误
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        // 2. Kafka 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-demo");

        // 3. 创建 Kafka 消费者
        FlinkKafkaConsumer<String> kafkaConsumer =
                new FlinkKafkaConsumer<>("test-topic", new SimpleStringSchema(), properties);

        // 4. 从 Kafka 读取数据
        DataStream<String> stream = env.addSource(kafkaConsumer);

        // 5. map 转对象
        DataStream<Person> personStream = stream.map(new MapFunction<String, Person>() {
            @Override
            public Person map(String value) throws Exception {
                String[] parts = value.split(",");
                return new Person(parts[0], Integer.parseInt(parts[1]));
            }
        });

        // 6. 过滤年龄 > 18
        DataStream<Person> adults = personStream.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person value) throws Exception {
                return value.age > 18;
            }
        });

        // 7. 打印结果
        adults.print();

        // 8. 执行任务
        env.execute("Flink Kafka Read Demo");
    }
}