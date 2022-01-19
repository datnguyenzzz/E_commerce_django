package vn.datnguyen.recommender.Handler;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Domain.Command;
import vn.datnguyen.recommender.Domain.DeleteRatingCommand;
import vn.datnguyen.recommender.Domain.ErrorRatingCommand;
import vn.datnguyen.recommender.Domain.PublishRatingCommand;
import vn.datnguyen.recommender.Domain.UpdateRatingCommand;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@Service
public class RatingService implements CommandHandler, EventHandler {

    private RatingTransactionalPublisher ratingPublisher;

    @Value("${transactionKafka.messageId}")
    private String partitionId;

    @Autowired
    public RatingService(RatingTransactionalPublisher ratingPublisher) {
        this.ratingPublisher = ratingPublisher;
    }

    @Override
    public CompletableFuture<Void> process(Command command) {
        return CompletableFuture.runAsync(() -> validate(command).forEach(ratingPublisher::execute));
    }

    private Command validate(Command command) {
        if (command instanceof PublishRatingCommand) {
            return validate((PublishRatingCommand) command);
        }
        else if (command instanceof UpdateRatingCommand) {
            return validate((UpdateRatingCommand) command);
        }
        else if (command instanceof DeleteRatingCommand) {
            return validate((DeleteRatingCommand) command);
        }
        return new ErrorRatingCommand(command.getClientId(), command.getItemId());
    }

    private List<AvroEvent> toAvroEvent(Command command) {
        if (command instanceof PublishRatingCommand) {
            return toAvroEvent((PublishRatingCommand) command);
        }
        else if (command instanceof UpdateRatingCommand) {
            return toAvroEvent((UpdateRatingCommand) command);
        }
        else {
            return toAvroEvent((DeleteRatingCommand) command);
        }
    }

    private Command validate(PublishRatingCommand command) {
        //int messageId = Integer.parseInt(partitionId);
        boolean acceptable = true;

        if (acceptable) {
            return command;
        }
        
        return new ErrorRatingCommand(command.getClientId(), command.getItemId());
    }

    private Command validate(UpdateRatingCommand command) {
        boolean acceptable = true;
        
        if (acceptable) {
            return command;
        }
        
        return new ErrorRatingCommand(command.getClientId(), command.getItemId());
    }

    private Command validate(DeleteRatingCommand command) {
        boolean acceptable = true;
        
        if (acceptable) {
            return command;
        }
        
        return new ErrorRatingCommand(command.getClientId(), command.getItemId());
    }

    private 


    @Override
    public CompletableFuture<Void> process(AvroEvent event) {
        // For saga invoked
        return null;
    }

    @Override
    public void apply(AvroEvent event) {
        //apply changes to aggregate
    }
}
