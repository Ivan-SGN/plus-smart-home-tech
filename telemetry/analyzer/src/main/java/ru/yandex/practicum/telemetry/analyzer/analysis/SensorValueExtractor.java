package ru.yandex.practicum.telemetry.analyzer.analysis;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionType;

import java.util.Optional;

@Component
public class SensorValueExtractor {

    public Optional<Integer> extract(ConditionType type, Object data) {
        return switch (type) {
            case TEMPERATURE -> switch (data) {
                case TemperatureSensorAvro temperature -> Optional.of(temperature.getTemperatureC());
                case ClimateSensorAvro climate -> Optional.of(climate.getTemperatureC());
                default -> Optional.empty();
            };
            case HUMIDITY -> data instanceof ClimateSensorAvro climate
                    ? Optional.of(climate.getHumidity()) : Optional.empty();
            case CO2LEVEL -> data instanceof ClimateSensorAvro climate
                    ? Optional.of(climate.getCo2Level()) : Optional.empty();
            case LUMINOSITY -> data instanceof LightSensorAvro light
                    ? Optional.of(light.getLuminosity()) : Optional.empty();
            case MOTION -> data instanceof MotionSensorAvro motion
                    ? Optional.of(toInt(motion.getMotion())) : Optional.empty();
            case SWITCH -> data instanceof SwitchSensorAvro switchSensor
                    ? Optional.of(toInt(switchSensor.getState())) : Optional.empty();
        };
    }

    private static int toInt(boolean value) {
        return value ? 1 : 0;
    }
}
