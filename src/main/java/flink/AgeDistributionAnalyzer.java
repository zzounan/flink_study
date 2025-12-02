package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

class AgeGroup {
    public String ageRange;
    public int count;

    public AgeGroup() {}

    public AgeGroup(String ageRange, int count) {
        this.ageRange = ageRange;
        this.count = count;
    }

    @Override
    public String toString() {
        return "AgeGroup{ageRange='" + ageRange + "', count=" + count + "}";
    }
}

public class AgeDistributionAnalyzer {

    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Kafka 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-age-distribution");

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

        // 6. 按年龄范围分组统计
        DataStream<Tuple2<String, Integer>> ageRangeCount = personStream
            .map(new MapFunction<Person, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(Person person) throws Exception {
                    String ageRange;
                    if (person.age < 18) {
                        ageRange = "0-17";
                    } else if (person.age < 30) {
                        ageRange = "18-29";
                    } else if (person.age < 45) {
                        ageRange = "30-44";
                    } else if (person.age < 60) {
                        ageRange = "45-59";
                    } else {
                        ageRange = "60+";
                    }
                    return new Tuple2<>(ageRange, 1);
                }
            })
            .keyBy(value -> value.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            .reduce((value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1));

        // 7. 打印年龄分布统计结果
        ageRangeCount.map(new MapFunction<Tuple2<String,Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> tuple) throws Exception {
                return "Age Range: " + tuple.f0 + ", Count: " + tuple.f1;
            }
        }).print();

        // 8. 执行任务
        env.execute("Flink Age Distribution Analyzer");
    }
}