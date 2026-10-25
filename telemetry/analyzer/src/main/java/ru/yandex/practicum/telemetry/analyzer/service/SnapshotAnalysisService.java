package ru.yandex.practicum.telemetry.analyzer.service;

import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.analysis.DeviceActionCommand;

import java.util.List;

public interface SnapshotAnalysisService {

    List<DeviceActionCommand> analyze(SensorsSnapshotAvro snapshot);
}
