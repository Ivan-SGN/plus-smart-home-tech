package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.telemetry.analyzer.exception.InvalidHubEventException;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.ActionType;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionOperation;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionType;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedEventHandler implements HubEventHandler<ScenarioAddedEventAvro> {

    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;

    @Override
    public Class<ScenarioAddedEventAvro> getPayloadType() {
        return ScenarioAddedEventAvro.class;
    }

    @Override
    @Transactional
    public void handle(String hubId, ScenarioAddedEventAvro payload) {
        Set<String> sensorIds = Stream.concat(
                        payload.getConditions().stream().map(ScenarioConditionAvro::getSensorId),
                        payload.getActions().stream().map(DeviceActionAvro::getSensorId))
                .collect(Collectors.toSet());

        Map<String, Sensor> sensors = sensorRepository.findByIdInAndHubId(sensorIds, hubId).stream()
                .collect(Collectors.toMap(Sensor::getId, Function.identity()));

        if (sensors.size() != sensorIds.size()) {
            throw new InvalidHubEventException("Хаб " + hubId + ": сценарий '" + payload.getName()
                    + "' ссылается на датчики, не зарегистрированные в этом хабе: "
                    + sensorIds.stream().filter(id -> !sensors.containsKey(id)).toList());
        }

        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, payload.getName())
                .orElseGet(() -> newScenario(hubId, payload.getName()));

        scenario.getConditions().clear();
        payload.getConditions().forEach(condition -> scenario.getConditions().add(
                new ScenarioCondition(scenario, sensors.get(condition.getSensorId()), toCondition(condition))));

        scenario.getActions().clear();
        payload.getActions().forEach(action -> scenario.getActions().add(
                new ScenarioAction(scenario, sensors.get(action.getSensorId()), toAction(action))));

        scenarioRepository.save(scenario);
        log.info("Хаб {}: сохранен сценарий '{}': условий {}, действий {}", hubId, payload.getName(),
                scenario.getConditions().size(), scenario.getActions().size());
    }

    private static Scenario newScenario(String hubId, String name) {
        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(name);
        return scenario;
    }

    private static Condition toCondition(ScenarioConditionAvro avro) {
        Condition condition = new Condition();
        condition.setType(ConditionType.valueOf(avro.getType().name()));
        condition.setOperation(ConditionOperation.valueOf(avro.getOperation().name()));
        condition.setValue(toInteger(avro.getValue()));
        return condition;
    }

    private static Action toAction(DeviceActionAvro avro) {
        Action action = new Action();
        action.setType(ActionType.valueOf(avro.getType().name()));
        action.setValue(avro.getValue());
        return action;
    }

    private static Integer toInteger(Object value) {
        return switch (value) {
            case null -> null;
            case Integer number -> number;
            case Boolean flag -> flag ? 1 : 0;
            default -> throw new InvalidHubEventException("Неподдерживаемое значение условия: " + value);
        };
    }
}
