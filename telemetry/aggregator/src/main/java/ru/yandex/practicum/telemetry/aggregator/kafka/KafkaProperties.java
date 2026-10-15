package ru.yandex.practicum.telemetry.aggregator.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("aggregator.kafka")
public record KafkaProperties(

        String bootstrapServers,

        Topics topics,

        ProducerSettings producer,

        ConsumerSettings consumer
) {
    public record Topics(

            String sensors,

            String snapshots
    ) {
    }

    public record ProducerSettings(

            String acks,

            int lingerMs,

            int batchSize,

            long maxBlockMs,

            int requestTimeoutMs,

            int deliveryTimeoutMs
    ) {
    }

    public record ConsumerSettings(

            String groupId,

            String autoOffsetReset,

            boolean enableAutoCommit,

            long pollTimeoutMs
    ) {
    }
}
