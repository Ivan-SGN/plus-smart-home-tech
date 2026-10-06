package ru.yandex.practicum.telemetry.collector.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.collector.exception.EventSendException;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
public class EventProducerImpl implements EventProducer, AutoCloseable {

    private final Producer<String, SpecificRecordBase> producer;
    private final long sendTimeoutMs;

    public EventProducerImpl(Producer<String, SpecificRecordBase> producer, KafkaProperties kafkaProperties) {
        this.producer = producer;
        this.sendTimeoutMs = kafkaProperties.producer().deliveryTimeoutMs();
    }

    @Override
    public void send(String topic, String key, SpecificRecordBase event) {
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(topic, key, event);
        try {
            Future<RecordMetadata> futureResult = producer.send(record);
            producer.flush();
            RecordMetadata metadata = futureResult.get(sendTimeoutMs, TimeUnit.MILLISECONDS);
            log.debug("Событие отправлено в топик {}, партиция {}, смещение {}",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventSendException(topic, key, e);
        } catch (ExecutionException e) {
            throw new EventSendException(topic, key, e.getCause());
        } catch (TimeoutException e) {
            throw new EventSendException(topic, key, e);
        }
    }

    @Override
    public void close() {
        log.info("Остановка producer'а: отправка оставшихся сообщений");
        producer.flush();
        producer.close(Duration.ofSeconds(10));
    }
}
