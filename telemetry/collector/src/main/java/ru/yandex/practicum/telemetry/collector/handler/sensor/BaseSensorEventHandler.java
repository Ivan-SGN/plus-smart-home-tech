package ru.yandex.practicum.telemetry.collector.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventRequest;

@Slf4j
public abstract class BaseSensorEventHandler<T extends SpecificRecordBase> implements SensorEventHandler {

    protected abstract T mapToAvro(SensorEventRequest event);

    @Override
    public final void handle(SensorEventRequest event) {
        SensorEventAvro avro = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(mapToAvro(event))
                .build();
        log.info("Событие датчика {} преобразовано в Avro: {}", getMessageType(), avro);
    }
}
