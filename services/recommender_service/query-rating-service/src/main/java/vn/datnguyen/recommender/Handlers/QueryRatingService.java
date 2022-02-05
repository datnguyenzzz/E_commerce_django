package vn.datnguyen.recommender.Handlers;

import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.AvroClasses.AvroQueryRating;
import vn.datnguyen.recommender.Models.Rating;
import vn.datnguyen.recommender.Repositories.RatingRepository;

@Service
public class QueryRatingService {

    private RatingTransactionalPublisher ratingTransactionalPublisher;
    private RatingRepository ratingRepository;

    @Value("${ConsumerKafka.partitionIdQueryRating}")
    private String partitionIdQueryRating;

    @Value("${incomingEvent.avroQueryRatingEvent}")
    private String avroQueryRatingEvent;

    @Autowired
    public QueryRatingService(RatingTransactionalPublisher ratingTransactionalPublisher,
                            RatingRepository ratingRepository) 
    {
        this.ratingTransactionalPublisher = ratingTransactionalPublisher;
        this.ratingRepository = ratingRepository;
    }

    public List<Rating> process(String clientId, String itemId) {
        if (clientId == null) {
            return ratingRepository.findByItemId(itemId);
        }
        else if (itemId == null) {
            return ratingRepository.findByClientId(clientId);
        }
        else {
            AvroEvent queryEvent = toAvroEvent(clientId, itemId);

            if (validate(queryEvent)) {
                ratingTransactionalPublisher.execute(queryEvent);
            }

            return ratingRepository.findByClientIdAndItemId(clientId, itemId);
        }
    }

    private AvroEvent toAvroEvent(String clientId, String itemId) {

        AvroQueryRating eventPayload = AvroQueryRating.newBuilder()
            .setClientId(clientId)
            .setItemId(itemId)
            .build();

        AvroEvent queryEvent = AvroEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setPartitionId(Integer.parseInt(partitionIdQueryRating))
            .setTimestamp(System.currentTimeMillis())
            .setEventType(avroQueryRatingEvent)
            .setData(eventPayload)
            .build();

        return queryEvent;
    }

    private boolean validate(AvroEvent event) {
        return true;
    }

}
