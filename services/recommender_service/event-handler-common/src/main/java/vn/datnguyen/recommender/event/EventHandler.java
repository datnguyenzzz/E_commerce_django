package vn.datnguyen.recommender.event;

import java.util.concurrent.CompletableFuture;

public interface EventHandler {
    CompletableFuture<Void> process(Event event);
}
