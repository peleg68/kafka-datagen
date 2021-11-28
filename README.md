# kafka-datagen

A simple Java tool to generate data and stream it to Kafka.

## Configuration

The app is configured using environment variables:

| Environment Variable | Default Value                      | Meaning                                                      |
| -------------------- | ---------------------------------- | ------------------------------------------------------------ |
| WAIT_TIME            | `250`                              | The wait time in between each event in milliseconds.         |
| LIMIT                | `2147483647` (`Integer.MAX_VALUE`) | A maximum limit of events to produce to Kafka.               |
| DRY_RUN              | `true`                             | When set to true, the application will print values instead of sending them to Kafka. |
| TOPIC_NAME           | `datagen`                          | The name of the Kafka topic to produce to.                   |
| BOOTSTRAP_SERVERS    | `localhost:9092`                   | Kafka cluster servers.                                       |
| CLIENT_ID            | `datagen`                          | Kafka client ID.                                             |
| KAFKA_TIMEOUT_MILLIS | `500`                              | Timeout for Kafka producer send operation in milliseconds.   |
