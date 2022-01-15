package vn.datnguyen.recommender.Converter;

import vn.datnguyen.recommender.Domain.DeleteRatingEvent;
import vn.datnguyen.recommender.Domain.Event;
import vn.datnguyen.recommender.Domain.PublishRatingEvent;
import vn.datnguyen.recommender.Domain.UpdateRatingEvent;
import vn.datnguyen.recommender.Serialization.AvroEvent;

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

    public AvroEvent from(PublishRatingEvent event) {
        return null;
    }

    public AvroEvent from(UpdateRatingEvent event) {
        return null;
    }

    public AvroEvent from (DeleteRatingEvent event) {
        return null;
    }
}
