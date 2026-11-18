package ru.yandex.practicum.telemetry.analyzer.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.exception.InvalidHubEventException;
import ru.yandex.practicum.telemetry.analyzer.handler.hub.HubEventHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class HubEventServiceImpl implements HubEventService {

    private final Map<Class<?>, HubEventHandler<?>> handlers;

    public HubEventServiceImpl(List<HubEventHandler<?>> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toMap(HubEventHandler::getPayloadType, Function.identity()));
    }

    @Override
    public void process(HubEventAvro event) {
        HubEventHandler<?> handler = handlers.get(event.getPayload().getClass());
        if (handler == null) {
            throw new InvalidHubEventException("Нет обработчика для события хаба " + event.getPayload().getClass().getSimpleName());
        }
        handle(handler, event);
    }

    private static <T> void handle(HubEventHandler<T> handler, HubEventAvro event) {
        handler.handle(event.getHubId(), handler.getPayloadType().cast(event.getPayload()));
    }
}
