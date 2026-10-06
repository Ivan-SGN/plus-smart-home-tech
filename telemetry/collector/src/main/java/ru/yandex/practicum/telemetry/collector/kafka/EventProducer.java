package ru.yandex.practicum.telemetry.collector.kafka;

import org.apache.avro.specific.SpecificRecordBase;

public interface EventProducer {

    void send(String topic, String key, SpecificRecordBase event);
}
