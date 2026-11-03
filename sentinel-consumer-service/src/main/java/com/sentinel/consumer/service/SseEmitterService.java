package com.sentinel.consumer.service;

import com.sentinel.consumer.dto.TelemetryStreamEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Slf4j
@Service
public class SseEmitterService {

  private static final long DEFAULT_SSE_TIMEOUT_MS = 0L;

  private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

  public SseEmitter createEmitter() {
    SseEmitter emitter = new SseEmitter(DEFAULT_SSE_TIMEOUT_MS);
    emitters.add(emitter);

    emitter.onCompletion(() -> emitters.remove(emitter));
    emitter.onTimeout(
        () -> {
          emitter.complete();
          emitters.remove(emitter);
        });
    emitter.onError(ex -> emitters.remove(emitter));

    try {
      emitter.send(
          SseEmitter.event()
              .name("connected")
              .data(
                  Map.of("status", "connected", "timestamp", System.currentTimeMillis()),
                  MediaType.APPLICATION_JSON));
    } catch (IOException ex) {
      log.debug("Failed to send initial SSE connected event", ex);
      emitters.remove(emitter);
    }

    return emitter;
  }

  public void broadcast(TelemetryStreamEvent event) {
    if (emitters.isEmpty()) {
      return;
    }

    List<SseEmitter> deadEmitters = new ArrayList<>();
    for (SseEmitter emitter : emitters) {
      try {
        emitter.send(SseEmitter.event().name("telemetry").data(event, MediaType.APPLICATION_JSON));
        if (event.alert() != null) {
          emitter.send(
              SseEmitter.event().name("alert").data(event.alert(), MediaType.APPLICATION_JSON));
        }
      } catch (Exception ex) {
        log.debug("Removing disconnected SSE client", ex);
        deadEmitters.add(emitter);
      }
    }

    if (!deadEmitters.isEmpty()) {
      emitters.removeAll(deadEmitters);
    }
  }

  public int getActiveClientCount() {
    return emitters.size();
  }
}
