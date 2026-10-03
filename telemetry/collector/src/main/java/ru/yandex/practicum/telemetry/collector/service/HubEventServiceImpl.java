package ru.yandex.practicum.telemetry.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEventRequest;

@Slf4j
@Service
public class HubEventServiceImpl implements HubEventService {

    @Override
    public void collect(HubEventRequest event) {
        log.info("Получено событие хаба: {}", event);
    }
}
