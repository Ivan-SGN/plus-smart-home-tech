package ru.yandex.practicum.telemetry.collector.handler.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventRequest;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaProperties;

public abstract class BaseSensorEventHandler<T extends SpecificRecordBase> implements SensorEventHandler {

    private final EventProducer eventProducer;
    private final String topic;

    protected BaseSensorEventHandler(EventProducer eventProducer, KafkaProperties kafkaProperties) {
        this.eventProducer = eventProducer;
        this.topic = kafkaProperties.topics().sensors();
    }

    protected abstract T mapToAvro(SensorEventRequest event);

    @Override
    public final void handle(SensorEventRequest event) {
        SensorEventAvro avro = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(mapToAvro(event))
                .build();
        eventProducer.send(topic, event.getHubId(), avro);
    }
}
