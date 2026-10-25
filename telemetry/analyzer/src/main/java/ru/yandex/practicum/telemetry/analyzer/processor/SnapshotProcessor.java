package ru.yandex.practicum.telemetry.analyzer.processor;

import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.analysis.DeviceActionCommand;
import ru.yandex.practicum.telemetry.analyzer.client.HubRouterClient;
import ru.yandex.practicum.telemetry.analyzer.kafka.KafkaProperties;
import ru.yandex.practicum.telemetry.analyzer.service.SnapshotAnalysisService;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final Consumer<String, SensorsSnapshotAvro> consumer;
    private final SnapshotAnalysisService snapshotAnalysisService;
    private final HubRouterClient hubRouterClient;
    private final KafkaProperties properties;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public void start() {
        registerShutdownHook(Thread.currentThread());

        String snapshotsTopic = properties.topics().snapshots();
        Duration pollTimeout = Duration.ofMillis(properties.snapshotConsumer().pollTimeoutMs());
        boolean failed = false;

        try {
            consumer.subscribe(List.of(snapshotsTopic));
            log.info("Обработка снапшотов запущена: {}", snapshotsTopic);

            while (true) {
                try {
                    consumer.poll(pollTimeout).forEach(this::handleRecord);
                } catch (RecordDeserializationException e) {
                    log.error("Не удалось десериализовать снапшот {}:{}, пропускаем", e.topicPartition(), e.offset(), e);
                    consumer.seek(e.topicPartition(), e.offset() + 1);
                    currentOffsets.put(e.topicPartition(), new OffsetAndMetadata(e.offset() + 1));
                    commitAsync();
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            failed = true;
            log.error("Ошибка во время обработки снапшотов, приложение будет остановлено", e);
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                log.info("Закрываем консьюмер снапшотов");
                consumer.close();
                if (failed) {
                    ApplicationExit.request();
                }
            }
        }
    }

    private void registerShutdownHook(Thread pollThread) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Остановка обработки снапшотов");
            consumer.wakeup();
            try {
                pollThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    private void handleRecord(ConsumerRecord<String, SensorsSnapshotAvro> record) {
        snapshotAnalysisService.analyze(record.value()).forEach(this::send);
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1));
        commitAsync();
    }

    private void send(DeviceActionCommand command) {
        try {
            hubRouterClient.send(command);
        } catch (StatusRuntimeException e) {
            log.warn("Хаб {}: команда сценария '{}' для {} не отправлена в Hub Router: {}",
                    command.hubId(), command.scenarioName(), command.sensorId(), e.getStatus());
        }
    }

    private void commitAsync() {
        consumer.commitAsync(currentOffsets, (offsets, e) -> {
            if (e != null) {
                log.warn("Не удалось зафиксировать смещения снапшотов: {}", offsets, e);
            }
        });
    }
}
