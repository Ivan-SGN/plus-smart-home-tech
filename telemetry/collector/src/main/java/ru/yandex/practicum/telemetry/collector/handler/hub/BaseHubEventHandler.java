package ru.yandex.practicum.telemetry.collector.handler.hub;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEventRequest;

@Slf4j
public abstract class BaseHubEventHandler<T extends SpecificRecordBase> implements HubEventHandler {

    protected abstract T mapToAvro(HubEventRequest event);

    @Override
    public final void handle(HubEventRequest event) {
        HubEventAvro avro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(mapToAvro(event))
                .build();
        log.info("Событие хаба {} преобразовано в Avro: {}", getMessageType(), avro);
    }
}
