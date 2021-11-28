package io.peleg;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Event {
    /**
     * A timestamp.
     */
    private Instant timestamp;

    /**
     * Represents how good the event is.
     */
    private Double goodness;

    /**
     * Represents the event type.
     * This will be used for partitioning the Kafka stream.
     */
    private Integer type;
}
