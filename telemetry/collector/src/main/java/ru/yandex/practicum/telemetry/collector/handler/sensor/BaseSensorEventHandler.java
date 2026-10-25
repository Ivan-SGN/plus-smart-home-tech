package ru.yandex.practicum.telemetry.collector.handler.sensor;

import com.google.protobuf.Timestamp;
import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaProperties;

import java.time.Instant;

public abstract class BaseSensorEventHandler<T extends SpecificRecordBase> implements SensorEventHandler {

    private final EventProducer eventProducer;
    private final String topic;

    protected BaseSensorEventHandler(EventProducer eventProducer, KafkaProperties kafkaProperties) {
        this.eventProducer = eventProducer;
        this.topic = kafkaProperties.topics().sensors();
    }

    protected abstract T mapToAvro(SensorEventProto event);

    @Override
    public final void handle(SensorEventProto event) {
        SensorEventAvro avro = SensorEventAvro.newBuilder()
                .setId(event.getId())
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