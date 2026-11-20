package ru.yandex.practicum.telemetry.analyzer.processor;

final class ApplicationExit {

    private ApplicationExit() {
    }

    static void request() {
        Thread exitThread = new Thread(() -> System.exit(1));
        exitThread.setName("AnalyzerExitThread");
        exitThread.start();
    }
}
