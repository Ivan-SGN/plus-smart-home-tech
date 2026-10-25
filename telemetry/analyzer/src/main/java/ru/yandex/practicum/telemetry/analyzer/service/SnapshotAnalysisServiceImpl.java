package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.analysis.DeviceActionCommand;
import ru.yandex.practicum.telemetry.analyzer.analysis.ScenarioAnalyzer;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
public class SnapshotAnalysisServiceImpl implements SnapshotAnalysisService {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioAnalyzer scenarioAnalyzer;

    @Override
    @Transactional(readOnly = true)
    public List<DeviceActionCommand> analyze(SensorsSnapshotAvro snapshot) {
        return scenarioAnalyzer.analyze(scenarioRepository.findByHubId(snapshot.getHubId()), snapshot);
    }
}
