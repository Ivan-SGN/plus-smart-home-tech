package ru.yandex.practicum.telemetry.analyzer.client;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("analyzer.hub-router")
public record HubRouterProperties(

        long deadlineMs
) {
}
