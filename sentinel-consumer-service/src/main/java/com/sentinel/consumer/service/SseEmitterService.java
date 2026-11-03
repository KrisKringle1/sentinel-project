package com.sentinel.consumer.service;

import com.sentinel.consumer.dto.TelemetryStreamEvent;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Slf4j
@Service
public class SseEmitterService {

  private static final long DEFAULT_SSE_TIMEOUT_MS = 0L;
  private static final int MAX_PENDING_EVENTS_PER_CLIENT = 100;

  private final List<ClientEmitter> emitters = new CopyOnWriteArrayList<>();
  private final ExecutorService broadcasterExecutor = Executors.newCachedThreadPool();

  public SseEmitter createEmitter() {
    SseEmitter emitter = new SseEmitter(DEFAULT_SSE_TIMEOUT_MS);
    ClientEmitter client = new ClientEmitter(emitter);
    emitters.add(client);

    emitter.onCompletion(() -> removeEmitter(client));
    emitter.onTimeout(
        () -> {
          emitter.complete();
          removeEmitter(client);
        });
    emitter.onError(ex -> removeEmitter(client));

    try {
      emitter.send(
          SseEmitter.event()
              .name("connected")
              .data(
                  Map.of("status", "connected", "timestamp", System.currentTimeMillis()),
                  MediaType.APPLICATION_JSON));
    } catch (IOException ex) {
      log.debug("Failed to send initial SSE connected event", ex);
      removeEmitter(client);
    }

    return emitter;
  }

  public void broadcast(TelemetryStreamEvent event) {
    if (emitters.isEmpty()) {
      return;
    }

    for (ClientEmitter emitter : emitters) {
      emitter.enqueue(event, MAX_PENDING_EVENTS_PER_CLIENT);
      emitter.drainAsync(broadcasterExecutor, this::removeEmitter);
    }
  }

  private void removeEmitter(ClientEmitter emitter) {
    if (emitters.remove(emitter)) {
      emitter.complete();
    }
  }

  public int getActiveClientCount() {
    return emitters.size();
  }

  @PreDestroy
  void shutdown() {
    broadcasterExecutor.shutdownNow();
  }

  private static final class ClientEmitter {
    private final SseEmitter emitter;
    private final Deque<TelemetryStreamEvent> queue = new ArrayDeque<>();
    private final AtomicBoolean draining = new AtomicBoolean(false);

    private ClientEmitter(SseEmitter emitter) {
      this.emitter = emitter;
    }

    private synchronized void enqueue(TelemetryStreamEvent event, int maxSize) {
      if (queue.size() >= maxSize) {
        queue.pollFirst();
      }
      queue.addLast(event);
    }

    private void drainAsync(
        ExecutorService executorService, java.util.function.Consumer<ClientEmitter> onFailure) {
      if (!draining.compareAndSet(false, true)) {
        return;
      }
      executorService.execute(() -> drainLoop(onFailure));
    }

    private void drainLoop(java.util.function.Consumer<ClientEmitter> onFailure) {
      try {
        while (true) {
          TelemetryStreamEvent event;
          synchronized (this) {
            event = queue.pollFirst();
          }
          if (event == null) {
            draining.set(false);
            synchronized (this) {
              if (queue.isEmpty() || !draining.compareAndSet(false, true)) {
                return;
              }
            }
            continue;
          }

          emitter.send(
              SseEmitter.event().name("telemetry").data(event, MediaType.APPLICATION_JSON));
          if (event.alert() != null) {
            emitter.send(
                SseEmitter.event().name("alert").data(event.alert(), MediaType.APPLICATION_JSON));
          }
        }
      } catch (Exception ex) {
        log.debug("Removing disconnected SSE client", ex);
        onFailure.accept(this);
      }
    }

    private void complete() {
      try {
        emitter.complete();
      } catch (Exception ignored) {
        // no-op
      }
    }
  }
}
