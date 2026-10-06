package ru.yandex.practicum.telemetry.collector.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.serializer.GeneralAvroSerializer;

import java.util.Properties;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaClientConfiguration {

    @Bean(destroyMethod = "close")
    public Producer<String, SpecificRecordBase> kafkaProducer(KafkaProperties properties) {
        KafkaProperties.ProducerSettings settings = properties.producer();

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GeneralAvroSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, settings.acks());
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.LINGER_MS_CONFIG, settings.lingerMs());
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, settings.batchSize());
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, settings.maxBlockMs());
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, settings.requestTimeoutMs());
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, settings.deliveryTimeoutMs());

        return new KafkaProducer<>(config);
    }
}
