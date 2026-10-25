package ru.yandex.practicum.telemetry.analyzer.handler.hub;

public interface HubEventHandler<T> {

    Class<T> getPayloadType();

    void handle(String hubId, T payload);
}
