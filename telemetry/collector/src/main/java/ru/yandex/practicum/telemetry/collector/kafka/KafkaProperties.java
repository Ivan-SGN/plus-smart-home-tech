package ru.yandex.practicum.telemetry.collector.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("collector.kafka")
public record KafkaProperties(

        String bootstrapServers,

        ProducerSettings producer,

        Topics topics
) {
    public record ProducerSettings(

            String acks,

            int lingerMs,

            int batchSize,

            long maxBlockMs,

            int requestTimeoutMs,

            int deliveryTimeoutMs
    ) {
    }

    public record Topics(

            String sensors,

            String hubs
    ) {
    }
}
