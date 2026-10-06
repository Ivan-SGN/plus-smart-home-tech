package ru.yandex.practicum.telemetry.collector.dto.sensor;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
public class ClimateSensorEventRequest extends SensorEventRequest {
    @NotNull(message = "Температура обязательна")
    private Integer temperatureC;

    @NotNull(message = "Влажность обязательна")
    private Integer humidity;

    @NotNull(message = "Уровень CO2 обязателен")
    private Integer co2Level;

    @Override
    public SensorEventType getType() {
        return SensorEventType.CLIMATE_SENSOR_EVENT;
    }
}
