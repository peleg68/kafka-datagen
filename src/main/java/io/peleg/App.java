package io.peleg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.util.Optional;
import java.util.Random;

public final class App {
    /**
     * Main method.
     * @param args
     */
    public static void main(final String[] args) throws InterruptedException, JsonProcessingException {
        int waitTime = getEnvVarInt("WAIT_TIME", 250);

        int limit = getEnvVarInt("LIMIT", Integer.MAX_VALUE);

        boolean dryRun = getEnvVarBool("DRY_RUN", true);

        Random random = new Random();
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        for (int i = 0; i < limit; i++) {
            String data = getRandomEventJson(random, mapper);

            if (dryRun) {
                System.out.println(data);
            } else {
                writeToKafka(data);
            }

            Thread.sleep(waitTime);
        }
    }

    private static void writeToKafka(String event) {

    }

    private static String getRandomEventJson(Random random, ObjectMapper objectMapper) throws JsonProcessingException {
        return objectMapper.writeValueAsString(createRandomEvent(random));
    }

    private static Event createRandomEvent(Random random){
        return Event.builder()
                .timestamp(Instant.now())
                .goodness(random.nextDouble())
                .type(random.nextInt(0,4))
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

    private static Boolean getEnvVarBool(final String name, final boolean fallback) {
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

    private App() { }
}
