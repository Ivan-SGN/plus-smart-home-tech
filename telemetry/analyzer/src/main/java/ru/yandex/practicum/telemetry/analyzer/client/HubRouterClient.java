package ru.yandex.practicum.telemetry.analyzer.client;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc.HubRouterControllerBlockingStub;
import ru.yandex.practicum.telemetry.analyzer.analysis.DeviceActionCommand;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class HubRouterClient {

    private final HubRouterControllerBlockingStub hubRouter;
    private final HubRouterProperties properties;

    public HubRouterClient(@GrpcClient("hub-router") HubRouterControllerBlockingStub hubRouter,
                           HubRouterProperties properties) {
        this.hubRouter = hubRouter;
        this.properties = properties;
    }

    public void send(DeviceActionCommand command) {
        hubRouter.withDeadlineAfter(properties.deadlineMs(), TimeUnit.MILLISECONDS)
                .handleDeviceAction(toRequest(command));
        log.info("Хаб {}: сценарий '{}' -> {} {}{}", command.hubId(), command.scenarioName(),
                command.sensorId(), command.type(), command.value() != null ? " " + command.value() : "");
    }

    private static DeviceActionRequest toRequest(DeviceActionCommand command) {
        DeviceActionProto.Builder action = DeviceActionProto.newBuilder()
                .setSensorId(command.sensorId())
                .setType(ActionTypeProto.valueOf(command.type().name()));
        if (command.value() != null) {
            action.setValue(command.value());
        }

        Instant now = Instant.now();
        return DeviceActionRequest.newBuilder()
                .setHubId(command.hubId())
                .setScenarioName(command.scenarioName())
                .setAction(action)
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano()))
                .build();
    }
}
