package ru.yandex.practicum.telemetry.collector.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.telemetry.collector.handler.sensor.SensorEventHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class SensorEventServiceImpl implements SensorEventService {

    private final Map<SensorEventProto.PayloadCase, SensorEventHandler> handlers;

    public SensorEventServiceImpl(List<SensorEventHandler> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toMap(SensorEventHandler::getMessageType, Function.identity()));
    }

    @Override
    public void collect(SensorEventProto event) {
        SensorEventHandler handler = handlers.get(event.getPayloadCase());
        if (handler == null) {
            throw new IllegalArgumentException("Нет обработчика для события датчика " + event.getPayloadCase());
        }
        handler.handle(event);
    }
}
