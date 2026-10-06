package ru.yandex.practicum.telemetry.collector.handler.hub;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEventRequest;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaProperties;

public abstract class BaseHubEventHandler<T extends SpecificRecordBase> implements HubEventHandler {

    private final EventProducer eventProducer;
    private final String topic;

    protected BaseHubEventHandler(EventProducer eventProducer, KafkaProperties kafkaProperties) {
        this.eventProducer = eventProducer;
        this.topic = kafkaProperties.topics().hubs();
    }

    protected abstract T mapToAvro(HubEventRequest event);

    @Override
    public final void handle(HubEventRequest event) {
        HubEventAvro avro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(mapToAvro(event))
                .build();
        eventProducer.send(topic, event.getHubId(), avro);
    }
}
