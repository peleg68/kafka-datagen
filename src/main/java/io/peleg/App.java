package io.peleg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Slf4j
public class App {
    //region default values
    /**
     * Default timeout for Kafka producer send operation in milliseconds.
     */
    private static final  int DEFAULT_KAFKA_TIMEOUT_MILLIS = 500;


    /**
     * The default wait time in between each event.
     */
    private static final int DEFAULT_WAIT_TIME = 250;
    //endregion

    //region lifecycle
    /**
     * The wait time in between each event in milliseconds.
     */
    private int waitTime;

    /**
     * A maximum limit of events to produce to Kafka.
     */
    private int limit;

    /**
     * When set to true, the application will print values
     * instead of sending them to Kafka.
     */
    private boolean dryRun;
    //endregion

    //region kafka
    /**
     * The name of the Kafka topic to produce to.
     */
    private String topicName;

    /**
     * Kafka cluster servers.
     */
    private String bootstrapServers;

    /**
     * Kafka client ID.
     */
    private String clientId;

    /**
     * Timeout for Kafka producer send operation in milliseconds.
     */
    private Long kafkaTimeoutMillis;
    //endregion

    //region resources
    /**
     * The random that will be used to generate random events.
     */
    private Random random;

    /**
     * The object mapper used to serialize events to JSON.
     */
    private ObjectMapper mapper;

    /**
     * Kafka producer.
     */
    private Producer<Integer, String> producer;
    //endregion


    /**
     * Default constructor.
     */
    public App() {
        setParamsFromEnv();
        random = new Random();
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        Properties kafkaProps = new Properties();
        kafkaProps.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);

        kafkaProps.put(
                ProducerConfig.CLIENT_ID_CONFIG,
                clientId);

        kafkaProps.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class.getName());

        kafkaProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        if (!dryRun) {
            producer = new KafkaProducer<>(kafkaProps);
        }
    }


    /**
     * @param args
     */
    public static void main(final String[] args)
            throws InterruptedException, JsonProcessingException {
        App app = new App();

        app.start();
    }

    /**
     * Start generating data and producing it to Kafka.
     * @throws JsonProcessingException
     * @throws InterruptedException
     */
    public void start() throws JsonProcessingException, InterruptedException {
        for (int i = 0; i < limit; i++) {
            String data = getRandomEventJson();

            if (dryRun) {
                log.info(data);
            } else {
                writeToKafka(data);
            }

            Thread.sleep(waitTime);
        }
    }

    private void setParamsFromEnv() {
        waitTime = getEnvVarInt("WAIT_TIME", DEFAULT_WAIT_TIME);
        limit = getEnvVarInt("LIMIT", Integer.MAX_VALUE);
        dryRun = getEnvVarBool("DRY_RUN", true);
        topicName = getEnvVar("TOPIC_NAME", "datagen");
        bootstrapServers = getEnvVar("BOOTSTRAP_SERVERS", "localhost:9092");
        clientId = getEnvVar("CLIENT_ID", "datagen");
        kafkaTimeoutMillis = Long.valueOf(getEnvVarInt(
                "KAFKA_TIMEOUT_MILLIS",
                DEFAULT_KAFKA_TIMEOUT_MILLIS));
    }

    private void writeToKafka(final String event) {
        ProducerRecord<Integer, String> record =
                new ProducerRecord<>(topicName, event);

        try {
            producer.send(record)
                    .get(kafkaTimeoutMillis, TimeUnit.MILLISECONDS);
            log.debug("Written record {} to Kafka", record);
        } catch (Exception e) {
            log.error("An error occurred sending to Kafka.", e);
        }
    }

    private String getRandomEventJson() throws JsonProcessingException {
        return mapper.writeValueAsString(createRandomEvent());
    }

    private Event createRandomEvent() {
        final int amountOfTypes = 3;

        return Event.builder()
                .timestamp(Instant.now())
                .goodness(random.nextDouble())
                .type(random.nextInt(0, amountOfTypes + 1))
                .build();
    }

    private static Optional<Integer> getEnvVarInt(final String name) {
        return Optional.ofNullable(System.getenv(name))
                .map(Integer::parseInt);
    }

    private static Integer getEnvVarInt(final String name, final int fallback) {
        return getEnvVarInt(name)
                .orElse(fallback);
    }

    private static Optional<Boolean> getEnvVarBool(final String name) {
        return Optional.ofNullable(System.getenv(name))
                .map(Boolean::parseBoolean);
    }

    private static Boolean getEnvVarBool(final String name,
                                         final boolean fallback) {
        return getEnvVarBool(name)
                .orElse(fallback);
    }

    private static Optional<String> getEnvVar(final String name) {
        return Optional.ofNullable(System.getenv(name));
    }

    private static String getEnvVar(final String name, final String fallback) {
        return getEnvVar(name)
                .orElse(fallback);
    }
}
