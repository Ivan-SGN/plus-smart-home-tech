package ru.yandex.practicum.telemetry.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventRequest;

@Slf4j
@Service
public class SensorEventServiceImpl implements SensorEventService {

    @Override
    public void collect(SensorEventRequest event) {
        log.info("Получено событие датчика: {}", event);
    }
}
