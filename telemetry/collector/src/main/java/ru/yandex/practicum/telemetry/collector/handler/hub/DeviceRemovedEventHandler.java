package ru.yandex.practicum.telemetry.collector.handler.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.telemetry.collector.dto.hub.DeviceRemovedEventRequest;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEventRequest;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEventType;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaProperties;

@Component
public class DeviceRemovedEventHandler extends BaseHubEventHandler<DeviceRemovedEventAvro> {

    public DeviceRemovedEventHandler(EventProducer eventProducer, KafkaProperties kafkaProperties) {
        super(eventProducer, kafkaProperties);
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.DEVICE_REMOVED;
    }

    @Override
    protected DeviceRemovedEventAvro mapToAvro(HubEventRequest event) {
        DeviceRemovedEventRequest deviceRemovedEvent = (DeviceRemovedEventRequest) event;
        return DeviceRemovedEventAvro.newBuilder()
                .setId(deviceRemovedEvent.getId())
                .build();
    }
}
