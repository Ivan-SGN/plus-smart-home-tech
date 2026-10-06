package ru.yandex.practicum.telemetry.collector.exception;

public class EventSendException extends RuntimeException {

    public EventSendException(String topic, String key, Throwable cause) {
        super("Не удалось отправить событие в топик " + topic + " с ключом " + key, cause);
    }
}
