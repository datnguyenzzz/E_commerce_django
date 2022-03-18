package vn.datnguyen.recommender.Handler;

import java.util.concurrent.CompletableFuture;

import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.Domain.Command;

@Service
public class QueryRecommendationService implements CommandHandler {
    
    public QueryRecommendationService() {}

    @Override
    public CompletableFuture<Object> process(Command command) {
        return CompletableFuture.supplyAsync(
            () -> {
                return command.toString();
            }
        );
    }
}
