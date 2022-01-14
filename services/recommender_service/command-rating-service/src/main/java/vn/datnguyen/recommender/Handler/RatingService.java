package vn.datnguyen.recommender.Handler;

import java.util.concurrent.CompletableFuture;

import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.Domain.Command;
import vn.datnguyen.recommender.Domain.Event;

@Service
public class RatingService implements CommandHandler, EventHandler {
    
    @Override
    public CompletableFuture<Void> process(Command command) {
        return null;
    }

    @Override
    public CompletableFuture<Void> process(Event event) {
        return null;
    }

    @Override
    public void apply(Event event) {}
}
