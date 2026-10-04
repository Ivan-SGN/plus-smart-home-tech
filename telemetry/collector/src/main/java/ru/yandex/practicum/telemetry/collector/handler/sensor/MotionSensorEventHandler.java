package ru.yandex.practicum.telemetry.collector.handler.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.telemetry.collector.dto.sensor.MotionSensorEventRequest;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventRequest;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaProperties;

@Component
public class MotionSensorEventHandler extends BaseSensorEventHandler<MotionSensorAvro> {

    public MotionSensorEventHandler(EventProducer eventProducer, KafkaProperties kafkaProperties) {
        super(eventProducer, kafkaProperties);
    }

    @Override
    public SensorEventType getMessageType() {
        return SensorEventType.MOTION_SENSOR_EVENT;
    }

    @Override
    protected MotionSensorAvro mapToAvro(SensorEventRequest event) {
        MotionSensorEventRequest motionEvent = (MotionSensorEventRequest) event;
        return MotionSensorAvro.newBuilder()
                .setLinkQuality(motionEvent.getLinkQuality())
                .setMotion(motionEvent.getMotion())
                .setVoltage(motionEvent.getVoltage())
                .build();
    }
}
