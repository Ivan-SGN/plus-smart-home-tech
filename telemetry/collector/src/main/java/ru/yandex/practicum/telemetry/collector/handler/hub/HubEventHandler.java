package ru.yandex.practicum.telemetry.collector.handler.hub;

import ru.yandex.practicum.telemetry.collector.dto.hub.HubEventRequest;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEventType;

public interface HubEventHandler {

    HubEventType getMessageType();

    void handle(HubEventRequest event);
}
