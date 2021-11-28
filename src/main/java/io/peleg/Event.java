package io.peleg;

import lombok.*;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Event {
    private Instant timestamp;
    private Double goodness;
    private Integer type;
}
