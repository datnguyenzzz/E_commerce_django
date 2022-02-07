package vn.datnguyen.recommender.Handler;

import java.util.concurrent.CompletableFuture;

import vn.datnguyen.recommender.Domain.Command;

public interface CommandHandler {
    CompletableFuture<Void> process(Command command);
}
