package io.peleg;

import lombok.*;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Event {
    private Instant timestamp;
    private Double goodness;
    private String type;

    private static Builder builder() {
        return new Builder();
    }

    public enum Types {
        FUN("fun"),
        DEEP("deep"),
        INTENSE("intense");
        
        Types (final String typeParam) {
            this.type = typeParam;
        }

        @Getter
        private String type;
    }

    public static class Builder {
        private Instant timestamp;
        private Double goodness;
        private String type;

        Builder() {
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder goodness(Double goodness) {
            this.goodness = goodness;
            return this;
        }

        private Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder fun() {
            return type(Types.FUN.getType());
        }

        public Builder deep() {
            return type(Types.DEEP.getType());
        }

        public Builder intense() {
            return type(Types.INTENSE.getType());
        }

        private Event build() {
            return new Event(timestamp, goodness, type);
        }

        public String toString() {
            return "Event.EventBuilder(timestamp=" + this.timestamp + ", goodness=" + this.goodness + ", type=" + this.type + ")";
        }
    }
}
