package ru.yandex.practicum.telemetry.collector.handler.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaProperties;

@Component
public class LightSensorEventHandler extends BaseSensorEventHandler<LightSensorAvro> {

    public LightSensorEventHandler(EventProducer eventProducer, KafkaProperties kafkaProperties) {
        super(eventProducer, kafkaProperties);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR;
    }

    @Override
    protected LightSensorAvro mapToAvro(SensorEventProto event) {
        LightSensorProto light = event.getLightSensor();
        return LightSensorAvro.newBuilder()
                .setLinkQuality(light.getLinkQuality())
                .setLuminosity(light.getLuminosity())
                .build();
    }
}
