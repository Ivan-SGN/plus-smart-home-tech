package ru.yandex.practicum.telemetry.collector.dto.hub;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@ToString(callSuper = true)
public class ScenarioAddedEventRequest extends HubEventRequest {
    @NotBlank(message = "Название сценария обязательно")
    @Size(min = 3, message = "Название сценария должно содержать не менее 3 символов")
    private String name;

    @NotEmpty(message = "Список условий не может быть пустым")
    private List<@Valid ScenarioConditionRequest> conditions;

    @NotEmpty(message = "Список действий не может быть пустым")
    private List<@Valid DeviceActionRequest> actions;

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED;
    }
}
