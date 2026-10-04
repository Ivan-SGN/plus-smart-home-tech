package ru.yandex.practicum.telemetry.collector.handler.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.telemetry.collector.dto.sensor.ClimateSensorEventRequest;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventRequest;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaProperties;

@Component
public class ClimateSensorEventHandler extends BaseSensorEventHandler<ClimateSensorAvro> {

    public ClimateSensorEventHandler(EventProducer eventProducer, KafkaProperties kafkaProperties) {
        super(eventProducer, kafkaProperties);
    }

    @Override
    public SensorEventType getMessageType() {
        return SensorEventType.CLIMATE_SENSOR_EVENT;
    }

    @Override
    protected ClimateSensorAvro mapToAvro(SensorEventRequest event) {
        ClimateSensorEventRequest climateEvent = (ClimateSensorEventRequest) event;
        return ClimateSensorAvro.newBuilder()
                .setTemperatureC(climateEvent.getTemperatureC())
                .setHumidity(climateEvent.getHumidity())
                .setCo2Level(climateEvent.getCo2Level())
                .build();
    }
}
