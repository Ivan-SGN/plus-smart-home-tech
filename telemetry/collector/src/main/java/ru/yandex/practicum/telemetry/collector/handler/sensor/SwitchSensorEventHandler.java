package ru.yandex.practicum.telemetry.collector.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventRequest;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SwitchSensorEventRequest;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaProperties;


@Component
public class SwitchSensorEventHandler extends BaseSensorEventHandler<SwitchSensorAvro> {

    public SwitchSensorEventHandler(EventProducer eventProducer, KafkaProperties kafkaProperties) {
        super(eventProducer, kafkaProperties);
    }

    @Override
    public SensorEventType getMessageType() {
        return SensorEventType.SWITCH_SENSOR_EVENT;
    }

    @Override
    protected SwitchSensorAvro mapToAvro(SensorEventRequest event) {
        SwitchSensorEventRequest sensorEvent = (SwitchSensorEventRequest) event;
        return SwitchSensorAvro.newBuilder()
                .setState(sensorEvent.getState())
                .build();
    }
}
