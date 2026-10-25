package ru.yandex.practicum.telemetry.analyzer.exception;

public class InvalidHubEventException extends RuntimeException {

    public InvalidHubEventException(String message) {
        super(message);
    }
}
