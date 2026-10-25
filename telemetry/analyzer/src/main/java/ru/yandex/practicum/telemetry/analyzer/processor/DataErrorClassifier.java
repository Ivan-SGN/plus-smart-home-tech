package ru.yandex.practicum.telemetry.analyzer.processor;

import org.springframework.dao.DataIntegrityViolationException;

import java.sql.SQLException;
import java.util.Set;

final class DataErrorClassifier {

    private static final Set<String> DATA_ERROR_SQL_STATE_CLASSES = Set.of("22", "23", "P0");

    private DataErrorClassifier() {
    }

    static boolean isDataError(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof DataIntegrityViolationException
                    || cause instanceof IllegalArgumentException
                    || isDataSqlState(cause)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDataSqlState(Throwable cause) {
        return cause instanceof SQLException sqlException
                && sqlException.getSQLState() != null
                && sqlException.getSQLState().length() >= 2
                && DATA_ERROR_SQL_STATE_CLASSES.contains(sqlException.getSQLState().substring(0, 2));
    }
}
