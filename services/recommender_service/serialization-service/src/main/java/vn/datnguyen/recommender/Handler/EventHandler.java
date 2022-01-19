package vn.datnguyen.recommender.Handler;

import java.util.concurrent.CompletableFuture;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;

public interface EventHandler {
    CompletableFuture<Void> process(AvroEvent event);
    void apply(AvroEvent event);
}
