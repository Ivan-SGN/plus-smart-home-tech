package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
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
    private final ConfigurableApplicationContext context;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    @Override
    public void run() {
        String hubsTopic = properties.topics().hubs();
        Duration pollTimeout = Duration.ofMillis(properties.hubConsumer().pollTimeoutMs());
        boolean failed = false;

        try {
            consumer.subscribe(List.of(hubsTopic));
            log.info("Обработка событий хабов запущена: {}", hubsTopic);

            while (true) {
                try {
                    consumer.poll(pollTimeout).forEach(this::handleRecord);
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
                    shutdownApplication();
                }
            }
        }
    }

    public void stop() {
        consumer.wakeup();
    }

    private void handleRecord(ConsumerRecord<String, HubEventAvro> record) {
        try {
            hubEventService.process(record.value());
        } catch (RuntimeException e) {
            if (!DataErrorClassifier.isDataError(e)) {
                throw e;
            }
            log.error("Событие хаба {} (offset {}) пропущено из-за ошибки данных: {}",
                    record.key(), record.offset(), record.value(), e);
        }
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1));
    }

    private void shutdownApplication() {
        Thread exitThread = new Thread(() -> System.exit(SpringApplication.exit(context, () -> 1)));
        exitThread.setName("AnalyzerExitThread");
        exitThread.start();
    }
}
