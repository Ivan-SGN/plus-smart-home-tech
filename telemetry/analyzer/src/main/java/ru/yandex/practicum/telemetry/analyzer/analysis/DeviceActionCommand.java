package ru.yandex.practicum.telemetry.analyzer.analysis;

import ru.yandex.practicum.telemetry.analyzer.model.ActionType;

public record DeviceActionCommand(

        String hubId,

        String scenarioName,

        String sensorId,

        ActionType type,

        Integer value
) {
}
