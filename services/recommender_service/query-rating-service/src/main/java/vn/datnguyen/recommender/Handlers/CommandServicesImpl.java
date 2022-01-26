package vn.datnguyen.recommender.Handlers;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Handler.EventHandler;
import vn.datnguyen.recommender.Models.Rating;
import vn.datnguyen.recommender.Repositories.ClientRatingRepository;

@Service
public class CommandServicesImpl implements CommandServices, EventHandler {
    
    private final Logger logger = LoggerFactory.getLogger(CommandServicesImpl.class);
    private ClientRatingRepository clientRatingRepository;

    @Autowired
    public CommandServicesImpl(ClientRatingRepository clientRatingRepository) {
        this.clientRatingRepository = clientRatingRepository;
    }

    @Override
    public CompletableFuture<Void> process(AvroEvent event) {
        return CompletableFuture.runAsync(
            () -> Stream.of(event)
                    .forEach(this::apply)
        );
    }

    @Override
    public void apply(AvroEvent event) {
        logger.info("QUERY-RATING-SERVICE: Consume event " + event);
    }

    /* 
        ADD NEW RATING 
    */
    @Override
    public void addNewClientRating(String clientId, String itemId, int score) {
        Rating rating = new Rating(clientId, itemId, score);
        addNewClientRating(rating);
    }

    public void addNewClientRating(Rating rating) {
        clientRatingRepository.save(rating);
    }
    
    /* 
        UPDATE EXIST RATING 
    */
    @Override
    public void updateClientRating(String clientId, String itemId, int score) {} 

    /* 
        DELETE RATING
    */
    @Override
    public void deleteClientRating(String clientId, String itemId) {}

    public void deleteClientRating(Rating rating) {
        clientRatingRepository.delete(rating);
    }

}
