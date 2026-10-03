package ru.yandex.practicum.telemetry.collector.dto.sensor;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
public class MotionSensorEventRequest extends SensorEventRequest {
    @NotNull(message = "Качество связи обязательно")
    private Integer linkQuality;

    @NotNull(message = "Признак движения обязателен")
    private Boolean motion;

    @NotNull(message = "Напряжение обязательно")
    private Integer voltage;

    @Override
    public SensorEventType getType() {
        return SensorEventType.MOTION_SENSOR_EVENT;
    }
}
