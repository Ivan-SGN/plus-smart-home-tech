package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.exception.InvalidHubEventException;
import ru.yandex.practicum.telemetry.analyzer.kafka.KafkaProperties;
import ru.yandex.practicum.telemetry.analyzer.service.HubEventService;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final Consumer<String, HubEventAvro> consumer;
    private final HubEventService hubEventService;
    private final KafkaProperties properties;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    @Override
    public void run() {
        registerShutdownHook(Thread.currentThread());

        String hubsTopic = properties.topics().hubs();
        Duration pollTimeout = Duration.ofMillis(properties.hubConsumer().pollTimeoutMs());
        boolean failed = false;

        try {
            consumer.subscribe(List.of(hubsTopic));
            log.info("Обработка событий хабов запущена: {}", hubsTopic);

            while (true) {
                try {
                    ConsumerRecords<String, HubEventAvro> records = consumer.poll(pollTimeout);
                    if (records.isEmpty()) {
                        continue;
                    }
                    records.forEach(this::handleRecord);
                } catch (RecordDeserializationException e) {
                    log.error("Не удалось десериализовать событие хаба {}:{}, пропускаем", e.topicPartition(), e.offset(), e);
                    consumer.seek(e.topicPartition(), e.offset() + 1);
                    currentOffsets.put(e.topicPartition(), new OffsetAndMetadata(e.offset() + 1));
                }
                consumer.commitAsync(currentOffsets, (offsets, e) -> {
                    if (e != null) {
                        log.warn("Не удалось зафиксировать смещения: {}", offsets, e);
                    }
                });
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            failed = true;
            log.error("Ошибка во время обработки событий хабов, приложение будет остановлено", e);
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                log.info("Закрываем консьюмер событий хабов");
                consumer.close();
                if (failed) {
                    ApplicationExit.request();
                }
            }
        }
    }

    private void registerShutdownHook(Thread pollThread) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Остановка обработки событий хабов");
            consumer.wakeup();
            try {
                pollThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    private void handleRecord(ConsumerRecord<String, HubEventAvro> record) {
        try {
            hubEventService.process(record.value());
        } catch (InvalidHubEventException e) {
            log.warn("Событие хаба {} (offset {}) пропущено: {}", record.key(), record.offset(), e.getMessage());
        }
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1));
    }
}
