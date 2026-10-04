package ru.yandex.practicum.telemetry.collector.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventRequest;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.handler.sensor.SensorEventHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class SensorEventServiceImpl implements SensorEventService {

    private final Map<SensorEventType, SensorEventHandler> handlers;

    public SensorEventServiceImpl(List<SensorEventHandler> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toMap(SensorEventHandler::getMessageType, Function.identity()));
    }

    @Override
    public void collect(SensorEventRequest event) {
        SensorEventHandler handler = handlers.get(event.getType());
        if (handler == null) {
            throw new IllegalArgumentException("Нет обработчика для события датчика " + event.getType());
        }
        handler.handle(event);
    }
}
