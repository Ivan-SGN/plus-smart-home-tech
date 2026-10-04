package ru.yandex.practicum.telemetry.collector.handler.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.telemetry.collector.dto.sensor.LightSensorEventRequest;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventRequest;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaProperties;

import java.util.Objects;

@Component
public class LightSensorEventHandler extends BaseSensorEventHandler<LightSensorAvro> {

    public LightSensorEventHandler(EventProducer eventProducer, KafkaProperties kafkaProperties) {
        super(eventProducer, kafkaProperties);
    }

    @Override
    public SensorEventType getMessageType() {
        return SensorEventType.LIGHT_SENSOR_EVENT;
    }

    @Override
    protected LightSensorAvro mapToAvro(SensorEventRequest event) {
        LightSensorEventRequest lightEvent = (LightSensorEventRequest) event;
        return LightSensorAvro.newBuilder()
                .setLinkQuality(Objects.requireNonNullElse(lightEvent.getLinkQuality(), 0))
                .setLuminosity(Objects.requireNonNullElse(lightEvent.getLuminosity(), 0))
                .build();
    }
}
