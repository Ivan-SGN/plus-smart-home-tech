package ru.yandex.practicum.telemetry.collector.handler.hub;

import com.google.protobuf.Timestamp;
import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaProperties;

import java.time.Instant;

public abstract class BaseHubEventHandler<T extends SpecificRecordBase> implements HubEventHandler {

    private final EventProducer eventProducer;
    private final String topic;

    protected BaseHubEventHandler(EventProducer eventProducer, KafkaProperties kafkaProperties) {
        this.eventProducer = eventProducer;
        this.topic = kafkaProperties.topics().hubs();
    }

    protected abstract T mapToAvro(HubEventProto event);

    @Override
    public final void handle(HubEventProto event) {
        HubEventAvro avro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(toInstant(event.getTimestamp()))
                .setPayload(mapToAvro(event))
                .build();
        eventProducer.send(topic, event.getHubId(), avro);
    }

    private static Instant toInstant(Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }
}
