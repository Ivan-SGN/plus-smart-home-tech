package ru.yandex.practicum.telemetry.collector.service;

import ru.yandex.practicum.telemetry.collector.dto.hub.HubEventRequest;

public interface HubEventService {

    void collect(HubEventRequest event);
}
