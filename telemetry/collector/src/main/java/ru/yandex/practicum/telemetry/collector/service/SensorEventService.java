package ru.yandex.practicum.telemetry.collector.service;

import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventRequest;

public interface SensorEventService {

    void collect(SensorEventRequest event);
}
