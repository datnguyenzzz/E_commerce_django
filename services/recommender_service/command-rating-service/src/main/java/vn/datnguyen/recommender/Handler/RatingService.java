package vn.datnguyen.recommender.Handler;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.Domain.Command;
import vn.datnguyen.recommender.Domain.DeleteRatingCommand;
import vn.datnguyen.recommender.Domain.DeleteRatingEvent;
import vn.datnguyen.recommender.Domain.Event;
import vn.datnguyen.recommender.Domain.PublishRatingCommand;
import vn.datnguyen.recommender.Domain.PublishRatingEvent;
import vn.datnguyen.recommender.Domain.UpdateRatingCommand;
import vn.datnguyen.recommender.Domain.UpdateRatingEvent;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@Service
public class RatingService implements CommandHandler, EventHandler {

    private RatingPublisher ratingPublisher;

    @Autowired
    public RatingService(RatingPublisher ratingPublisher) {
        this.ratingPublisher = ratingPublisher;
    }

    @Override
    public CompletableFuture<Void> process(Command command) {
        return CompletableFuture.runAsync(() -> validate(command).forEach(ratingPublisher::execute));
    }

    private List<Event> validate(Command command) {
        if (command instanceof PublishRatingCommand) {
            return validate((PublishRatingCommand) command);
        }
        else if (command instanceof UpdateRatingCommand) {
            return validate((UpdateRatingCommand) command);
        }
        else if (command instanceof DeleteRatingCommand) {
            return validate((DeleteRatingCommand) command);
        }
        return emptyList();
    }

    private List<Event> validate(PublishRatingCommand command) {
        return singletonList(new PublishRatingEvent(command.getClientId(), command.getItemId(), command.getScore()));
    }

    private List<Event> validate(UpdateRatingCommand command) {
        return singletonList(new UpdateRatingEvent(command.getClientId(), command.getItemId(), command.getScore()));
    }

    private List<Event> validate(DeleteRatingCommand command) {
        return singletonList(new DeleteRatingEvent(command.getClientId(), command.getItemId()));
    }

    @Override
    public CompletableFuture<Void> process(Event event) {
        // For saga invoked
        return null;
    }

    @Override
    public void apply(Event event) {
        //apply changes to aggregate
    }
}
