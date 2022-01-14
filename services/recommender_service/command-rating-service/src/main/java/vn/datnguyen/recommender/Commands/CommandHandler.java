package vn.datnguyen.recommender.Commands;

import java.util.concurrent.CompletableFuture;

public interface CommandHandler {
    CompletableFuture<Void> process(Command command);
}
