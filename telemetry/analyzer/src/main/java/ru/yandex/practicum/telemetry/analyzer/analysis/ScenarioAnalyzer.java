package ru.yandex.practicum.telemetry.analyzer.analysis;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioCondition;

import java.util.List;

@Component
@RequiredArgsConstructor
public class ScenarioAnalyzer {

    private final SensorValueExtractor valueExtractor;

    public List<DeviceActionCommand> analyze(List<Scenario> scenarios, SensorsSnapshotAvro snapshot) {
        return scenarios.stream()
                .filter(scenario -> !scenario.getConditions().isEmpty())
                .filter(scenario -> scenario.getConditions().stream().allMatch(link -> isMet(link, snapshot)))
                .flatMap(scenario -> scenario.getActions().stream().map(link -> toCommand(scenario, link)))
                .toList();
    }

    private boolean isMet(ScenarioCondition link, SensorsSnapshotAvro snapshot) {
        SensorStateAvro state = snapshot.getSensorsState().get(link.getSensor().getId());
        Condition condition = link.getCondition();
        if (state == null || condition.getValue() == null) {
            return false;
        }
        return valueExtractor.extract(condition.getType(), state.getData())
                .map(actual -> condition.getOperation().test(actual, condition.getValue()))
                .orElse(false);
    }

    private static DeviceActionCommand toCommand(Scenario scenario, ScenarioAction link) {
        return new DeviceActionCommand(
                scenario.getHubId(),
                scenario.getName(),
                link.getSensor().getId(),
                link.getAction().getType(),
                link.getAction().getValue());
    }
}
