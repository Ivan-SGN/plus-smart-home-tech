package ru.yandex.practicum.telemetry.collector.exception;

import io.grpc.Status;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.advice.GrpcAdvice;
import net.devh.boot.grpc.server.advice.GrpcExceptionHandler;

@Slf4j
@GrpcAdvice
public class GrpcExceptionAdvice {

    @GrpcExceptionHandler(IllegalArgumentException.class)
    public Status handleInvalidArgument(IllegalArgumentException e) {
        log.warn("Некорректное событие: {}", e.getMessage());
        return Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e);
    }

    @GrpcExceptionHandler(EventSendException.class)
    public Status handleEventSend(EventSendException e) {
        log.error("Ошибка отправки события в Kafka", e);
        return Status.UNAVAILABLE.withDescription(e.getMessage()).withCause(e);
    }

    @GrpcExceptionHandler(Exception.class)
    public Status handleUnexpected(Exception e) {
        log.error("Непредвиденная ошибка при обработке события", e);
        return Status.INTERNAL.withDescription(e.getMessage()).withCause(e);
    }
}
