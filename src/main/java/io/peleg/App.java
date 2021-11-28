package io.peleg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.Optional;
import java.util.Random;

public final class App {
    /**
     * The default wait time in between each event.
     */
    private static final int DEFAULT_WAIT_TIME = 250;

    /**
     * The wait time in between each event.
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

    /**
     * The name of the Kafka topic to produce to.
     */
    private String topicName;

    /**
     * The random that will be used to generate random events.
     */
    private Random random;

    /**
     * The object mapper used to serialize events to JSON.
     */
    private ObjectMapper mapper;

    /**
     * Default constructor.
     */
    public App() {
        setParamsFromEnv();
        random = new Random();
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
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
                System.out.println(data);
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
    }

    private void writeToKafka(final String event) {
        ProducerRecord<Integer, String> record =
                new ProducerRecord<>(topicName, event);
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
