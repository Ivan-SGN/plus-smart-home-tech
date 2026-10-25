package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceRemovedEventHandler implements HubEventHandler<DeviceRemovedEventAvro> {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;

    @Override
    public Class<DeviceRemovedEventAvro> getPayloadType() {
        return DeviceRemovedEventAvro.class;
    }

    @Override
    @Transactional
    public void handle(String hubId, DeviceRemovedEventAvro payload) {
        sensorRepository.findByIdAndHubId(payload.getId(), hubId)
                .ifPresent(sensor -> remove(hubId, sensor));
    }

    private void remove(String hubId, Sensor sensor) {
        List<Scenario> affected = scenarioRepository.findByHubId(hubId).stream()
                .filter(scenario -> usesSensor(scenario, sensor.getId()))
                .toList();
        scenarioRepository.deleteAll(affected);
        sensorRepository.delete(sensor);
        log.info("Хаб {}: удален датчик {} и сценарии с ним: {}", hubId, sensor.getId(),
                affected.stream().map(Scenario::getName).toList());
    }

    private static boolean usesSensor(Scenario scenario, String sensorId) {
        return scenario.getConditions().stream().anyMatch(link -> link.getSensor().getId().equals(sensorId))
                || scenario.getActions().stream().anyMatch(link -> link.getSensor().getId().equals(sensorId));
    }
}
