# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Java Maven project using Apache Flink for stream processing with Kafka integration. The main purpose appears to be a study project for learning Flink streaming capabilities with Kafka as a data source.

Key components:
- Apache Flink 1.16.0 for stream processing
- Kafka connector for Flink
- Sample Flink job that reads from Kafka, processes data, and filters results
- Kafka test producer for generating sample data

## Project Structure

```
src/
├── main/
│   ├── java/
│   │   ├── flink/KafkaReadDemo.java          # Main Flink streaming job
│   │   ├── flink/AgeDistributionAnalyzer.java # New Flink job for age distribution analysis
│   │   ├── flink/Person.java                 # Person data model
│   │   ├── kafka/KafkaTestProducer.java      # Utility to produce test data to Kafka
│   │   └── org/example/App.java             # Basic Hello World application
└── test/
    └── java/
        └── org/example/AppTest.java         # Basic unit test
```

## Key Classes

1. `flink.KafkaReadDemo` - Main Flink application that:
   - Consumes messages from Kafka topic "test-topic"
   - Parses comma-separated name and age values
   - Maps them to Person objects
   - Filters for adults (age > 18)
   - Prints results to console

2. `flink.AgeDistributionAnalyzer` - New Flink application that:
   - Consumes messages from Kafka topic "test-topic"
   - Parses comma-separated name and age values
   - Groups people by age ranges (0-17, 18-29, 30-44, 45-59, 60+)
   - Calculates distribution statistics every 10 seconds
   - Prints age distribution results to console

3. `kafka.KafkaTestProducer` - Utility class that:
   - Produces random name and age data to Kafka topic "test-topic"
   - Runs in an infinite loop sending data every second

## Build System

Maven is used for dependency management and building. Key dependencies include:
- Apache Flink streaming Java API
- Flink Kafka connector
- JUnit for testing

## Common Development Commands

### Building the Project
```bash
mvn clean compile
```

### Running Tests
```bash
mvn test
```

### Packaging the Application
```bash
mvn package
```

### Running the Applications

1. Start the Kafka test producer:
```bash
mvn exec:java -Dexec.mainClass="kafka.KafkaTestProducer"
```

2. Run the Flink Kafka consumer:
```bash
mvn exec:java -Dexec.mainClass="flink.KafkaReadDemo"
```

3. Run the Age Distribution Analyzer (new feature):
```bash
mvn exec:java -Dexec.mainClass="flink.AgeDistributionAnalyzer"
```

Note: All applications require a running Kafka instance at localhost:9092.

## Architecture Notes

This is a simple streaming data processing example:
1. Kafka acts as the message broker/source
2. Flink processes the stream in real-time
3. Simple transformations and filtering are applied
4. Results are printed to console

The project follows standard Maven directory structure and uses minimal dependencies focused on the core Flink and Kafka functionality.