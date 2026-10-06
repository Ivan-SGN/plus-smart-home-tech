package ru.yandex.practicum.telemetry.collector.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEventRequest;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEventType;
import ru.yandex.practicum.telemetry.collector.handler.hub.HubEventHandler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class HubEventServiceImpl implements HubEventService {

    private final Map<HubEventType, HubEventHandler> handlers;

    public HubEventServiceImpl(List<HubEventHandler> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toMap(HubEventHandler::getMessageType, Function.identity()));
    }

    @Override
    public void collect(HubEventRequest event) {
        HubEventHandler handler = handlers.get(event.getType());
        if (handler == null) {
            throw new IllegalArgumentException("Нет обработчика для события хаба " + event.getType());
        }
        handler.handle(event);
    }
}
