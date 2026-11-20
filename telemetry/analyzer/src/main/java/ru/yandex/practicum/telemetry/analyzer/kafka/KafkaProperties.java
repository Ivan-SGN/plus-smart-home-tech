package ru.yandex.practicum.telemetry.analyzer.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("analyzer.kafka")
public record KafkaProperties(

        String bootstrapServers,

        Topics topics,

        ConsumerSettings hubConsumer,

        ConsumerSettings snapshotConsumer
) {
    public record Topics(

            String hubs,

            String snapshots
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
