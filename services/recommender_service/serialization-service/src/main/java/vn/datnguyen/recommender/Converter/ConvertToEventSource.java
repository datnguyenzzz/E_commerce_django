package vn.datnguyen.recommender.Converter;

import org.springframework.stereotype.Component;

import vn.datnguyen.recommender.Domain.DeleteRatingEvent;
import vn.datnguyen.recommender.Domain.Event;
import vn.datnguyen.recommender.Domain.PublishRatingEvent;
import vn.datnguyen.recommender.Domain.UpdateRatingEvent;
import vn.datnguyen.recommender.Serialization.AvroDeleteRating;
import vn.datnguyen.recommender.Serialization.AvroEvent;
import vn.datnguyen.recommender.Serialization.AvroPublishRating;
import vn.datnguyen.recommender.Serialization.AvroUpdateRating;

@Component
public class ConvertToEventSource {
    public ConvertToEventSource() {}

    public AvroEvent from(Event event) {
        if (event instanceof PublishRatingEvent) {
            return from((PublishRatingEvent) event);
        }
        else if (event instanceof UpdateRatingEvent) {
            return from((UpdateRatingEvent) event);
        }
        else {
            return from((DeleteRatingEvent) event);
        }
    }

    private AvroEvent from(PublishRatingEvent event) {
        AvroPublishRating eventPayload = AvroPublishRating.newBuilder()
            .setClientId(event.getClientId())
            .setItemId(event.getItemId())
            .setScore(event.getScore())
            .build();
                                                        
        return wrap(event, eventPayload);
    }

    private AvroEvent from(UpdateRatingEvent event) {
        AvroUpdateRating eventPayload = AvroUpdateRating.newBuilder()
            .setClientId(event.getClientId())
            .setItemId(event.getItemId())
            .setScore(event.getScore())
            .build();

        return wrap(event, eventPayload);
    }

    private AvroEvent from(DeleteRatingEvent event) {
        AvroDeleteRating eventPayload = AvroDeleteRating.newBuilder()
            .setClientId(event.getClientId())
            .setItemId(event.getItemId())
            .build();

        return wrap(event, eventPayload);
    }

    private AvroEvent wrap(Event event, Object payload){
        return AvroEvent.newBuilder()
                        .setEventId(event.getEventId())
                        .setTimestamp(event.getTimestamp())
                        .setPartitionId(event.getPartitionId())
                        .setData(payload)
                        .build();
    }
}
