package ru.yandex.practicum.telemetry.collector.service;

import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

public interface SensorEventService {

    void collect(SensorEventProto event);
}
