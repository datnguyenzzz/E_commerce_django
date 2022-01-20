package vn.datnguyen.recommender.Handler;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.AvroClasses.AvroDeleteRating;
import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.AvroClasses.AvroPublishRating;
import vn.datnguyen.recommender.AvroClasses.AvroUpdateRating;
import vn.datnguyen.recommender.Domain.Command;
import vn.datnguyen.recommender.Domain.DeleteRatingCommand;
import vn.datnguyen.recommender.Domain.ErrorRatingCommand;
import vn.datnguyen.recommender.Domain.PublishRatingCommand;
import vn.datnguyen.recommender.Domain.UpdateRatingCommand;

@Service
public class RatingService implements CommandHandler, EventHandler {

    private RatingTransactionalPublisher ratingPublisher;

    @Value("${transactionKafka.messageId}")
    private String partitionId;

    @Value("${incomingEvent.avroPublishRatingEvent}")
    private String avroPublishRatingEvent;

    @Value("${incomingEvent.avroUpdateRatingEvent}")
    private String avroUpdateRatingEvent;

    @Value("${incomingEvent.avroDeleteRatingEvent}")
    private String avroDeleteRatingEvent;

    @Autowired
    public RatingService(RatingTransactionalPublisher ratingPublisher) {
        this.ratingPublisher = ratingPublisher;
    }

    @Override
    public CompletableFuture<Void> process(Command command) {
        return CompletableFuture.runAsync(
            () -> Stream.of(command).map(this::validate)
                                    .filter(this::isNotErrorCommand)
                                    .map(this::toAvroEvent)
                                    .forEach(ratingPublisher::execute)
        );
    }

    private boolean isNotErrorCommand(Command command) {
        return (!(command instanceof ErrorRatingCommand));
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
        return new ErrorRatingCommand("Undefined error RATING-COMMAND-SERVICE");
    }

    private AvroEvent toAvroEvent(Command command) {
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
        boolean acceptable = true;

        if (acceptable) {
            return command;
        }
        
        return new ErrorRatingCommand(command.getClientId(), command.getItemId(), "Unacceptable publish rating");
    }

    private Command validate(UpdateRatingCommand command) {
        boolean acceptable = true;
        
        if (acceptable) {
            return command;
        }
        
        return new ErrorRatingCommand(command.getClientId(), command.getItemId(), "Unacceptable update rating");
    }

    private Command validate(DeleteRatingCommand command) {
        boolean acceptable = true;
        
        if (acceptable) {
            return command;
        }
        
        return new ErrorRatingCommand(command.getClientId(), command.getItemId(), "Unacceptable delete rating");
    }

    private AvroEvent toAvroEvent(PublishRatingCommand command) {
        AvroPublishRating eventPayload = AvroPublishRating.newBuilder()
            .setClientId(command.getClientId())
            .setItemId(command.getItemId())
            .setScore(command.getScore())
            .build();
        
        return wrap(eventPayload, avroPublishRatingEvent);
    }

    private AvroEvent toAvroEvent(UpdateRatingCommand command) {
        AvroUpdateRating eventPayload = AvroUpdateRating.newBuilder()
            .setClientId(command.getClientId())
            .setItemId(command.getItemId())
            .setScore(command.getScore())
            .build();
        
        return wrap(eventPayload, avroUpdateRatingEvent);
    }

    private AvroEvent toAvroEvent(DeleteRatingCommand command) {
        AvroDeleteRating eventPayload = AvroDeleteRating.newBuilder()
            .setClientId(command.getClientId())
            .setItemId(command.getItemId())
            .build();
        
        return wrap(eventPayload, avroDeleteRatingEvent);
    }

    private AvroEvent wrap(Object payload, String payloadType) {
        return AvroEvent.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setPartitionId(Integer.parseInt(partitionId))
                        .setTimestamp(System.currentTimeMillis())
                        .setEventType(payloadType)
                        .setData(payload)
                        .build();
    }

    @Override
    public CompletableFuture<Void> process(AvroEvent event) {
        return null;
    }

    @Override
    public void apply(AvroEvent event) {
        //apply changes to aggregate
    }
}
