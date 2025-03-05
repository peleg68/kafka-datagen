package io.peleg;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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


    /**
     * The default wait time in between each event.
     */
    private static final String DEFAULT_OUTPUT_FORMAT = "JSON";
    //endregion

    //region lifecycle
    /**
     * The output format - possible values are "JSON" or "AVRO".
     */
    private String outputFormat;

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

    /**
     * When set to true and dry run is enabled - the values will be printed in Base64
     */
    private boolean dryRunPrintInBase64;
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
     * The writer used for serialization.
     */
    private DatumWriter<Event> writer;

    /**
     * The JSON/Avro encoder.
     */
    private Encoder encoder;

    /**
     * The output stream from which data will be read before producing to Kafka.
     */
    private ByteArrayOutputStream outputStream;

    /**
     * Kafka producer.
     */
    private Producer<Integer, byte[]> producer;
    //endregion


    /**
     * Default constructor.
     */
    public App() throws IOException {
        setParamsFromEnv();
        random = new Random();
        writer = new SpecificDatumWriter<>(Event.class);
        outputStream = new ByteArrayOutputStream();
        switch (outputFormat) {
            case "JSON":
                encoder = EncoderFactory.get().jsonEncoder(Event.getClassSchema(), outputStream, false);
                dryRunPrintInBase64 = false;
                break;
            case "AVRO":
                encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                dryRunPrintInBase64 = true;
                break;
        }

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
            throws InterruptedException, IOException {
        App app = new App();

        app.start();
    }

    /**
     * Start generating data and producing it to Kafka.
     * @throws IOException
     * @throws InterruptedException
     */
    public void start() throws IOException, InterruptedException {
        for (int i = 0; i < limit; i++) {
            byte[] data = getRandomEventSerialized();

            if (dryRun) {
                String printData = dryRunPrintInBase64 ? new SpecificDatumReader<>(Event.class).read(null, DecoderFactory.get().binaryDecoder(data, null)).toString() : new String(data);
                log.info(printData);
            } else {
                writeToKafka(data);
            }

            Thread.sleep(waitTime);
        }
    }

    private void setParamsFromEnv() {
        outputFormat = getEnvVar("OUTPUT_FORMAT", DEFAULT_OUTPUT_FORMAT);
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

    private void writeToKafka(final byte[] event) {
        ProducerRecord<Integer, byte[]> record =
                new ProducerRecord<>(topicName, event);

        try {
            producer.send(record)
                    .get(kafkaTimeoutMillis, TimeUnit.MILLISECONDS);
            log.debug("Written record {} to Kafka", record);
        } catch (Exception e) {
            log.error("An error occurred sending to Kafka.", e);
        }
    }

    private byte[] getRandomEventSerialized() {
        return serialize(createRandomEvent());
    }

    public byte[] serialize(Event avroObject) {
        try {
            writer.write(avroObject, encoder);
            encoder.flush();
            byte[] serializedBytes = outputStream.toByteArray();
            outputStream.reset();
            return serializedBytes;
        } catch (IOException e) {
            log.error("An error occurred serializing Event object.", e);
            return new byte[0];
        }
    }

    private Event createRandomEvent() {
        final int amountOfTypes = 3;

        return Event.newBuilder()
                .setTimestamp(Instant.now())
                .setGoodness(random.nextDouble())
                .setType(random.nextInt(0, amountOfTypes + 1))
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
