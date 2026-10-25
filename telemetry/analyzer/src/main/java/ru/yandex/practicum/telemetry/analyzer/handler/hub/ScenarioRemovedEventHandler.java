package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioRemovedEventHandler implements HubEventHandler<ScenarioRemovedEventAvro> {

    private final ScenarioRepository scenarioRepository;

    @Override
    public Class<ScenarioRemovedEventAvro> getPayloadType() {
        return ScenarioRemovedEventAvro.class;
    }

    @Override
    @Transactional
    public void handle(String hubId, ScenarioRemovedEventAvro payload) {
        scenarioRepository.findByHubIdAndName(hubId, payload.getName()).ifPresent(scenario -> {
            scenarioRepository.delete(scenario);
            log.info("Хаб {}: удален сценарий '{}'", hubId, payload.getName());
        });
    }
}
