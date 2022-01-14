package vn.datnguyen.recommender.Handler;

import java.util.concurrent.CompletableFuture;

import vn.datnguyen.recommender.Domain.Event;

public interface EventHandler {
    CompletableFuture<Void> process(Event event);
    void apply(Event event);
}
