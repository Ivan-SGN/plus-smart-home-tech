package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceAddedEventHandler implements HubEventHandler<DeviceAddedEventAvro> {

    private final SensorRepository sensorRepository;

    @Override
    public Class<DeviceAddedEventAvro> getPayloadType() {
        return DeviceAddedEventAvro.class;
    }

    @Override
    @Transactional
    public void handle(String hubId, DeviceAddedEventAvro payload) {
        if (sensorRepository.existsById(payload.getId())) {
            log.debug("Датчик {} уже зарегистрирован, пропускаем", payload.getId());
            return;
        }

        Sensor sensor = new Sensor();
        sensor.setId(payload.getId());
        sensor.setHubId(hubId);
        sensorRepository.save(sensor);
        log.info("Хаб {}: добавлен датчик {}", hubId, payload.getId());
    }
}
