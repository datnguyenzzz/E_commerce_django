package vn.datnguyen.recommender.Handler;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.AvroClasses.AvroAddToCartBehavior;
import vn.datnguyen.recommender.AvroClasses.AvroBuyBehavior;
import vn.datnguyen.recommender.AvroClasses.AvroDeleteRating;
import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.AvroClasses.AvroPublishRating;
import vn.datnguyen.recommender.AvroClasses.AvroUpdateRating;
import vn.datnguyen.recommender.Domain.AddToCartBehaviorCommand;
import vn.datnguyen.recommender.Domain.BuyBehaviorCommand;
import vn.datnguyen.recommender.Domain.Command;
import vn.datnguyen.recommender.Domain.DeleteRatingCommand;
import vn.datnguyen.recommender.Domain.ErrorRatingCommand;
import vn.datnguyen.recommender.Domain.PublishRatingCommand;
import vn.datnguyen.recommender.Domain.UpdateRatingCommand;

@Service
public class RatingService implements CommandHandler, EventHandler {

    private final static int MAX_SCORE = 10; 
    private final static int MIN_SCORE = 1;

    private RatingTransactionalPublisher ratingPublisher;

    @Value("${transactionKafka.partitionIdCommandRating}")
    private String partitionIdCommandRating;

    @Value("${transactionKafka.partitionIdBuyBehavior}")
    private String partitionIdBuyBehavior;

    @Value("${transactionKafka.partitionIdAddToCartBehavior}")
    private String partitionIdAddToCartBehavior;

    @Value("${incomingEvent.avroPublishRatingEvent}")
    private String avroPublishRatingEvent;

    @Value("${incomingEvent.avroUpdateRatingEvent}")
    private String avroUpdateRatingEvent;

    @Value("${incomingEvent.avroDeleteRatingEvent}")
    private String avroDeleteRatingEvent;

    @Value("${incomingEvent.avroBuyBehaviorEvent}")
    private String avroBuyBehaviorEvent;

    @Value("${incomingEvent.avroAddToCartBehaviorEvent}")
    private String avroAddToCartBehaviorEvent;

    private Logger logger = LoggerFactory.getLogger(RatingService.class);

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
        if (command instanceof ErrorRatingCommand) {
            logger.warn("*****ERROR COMMAND !!!! *****" + command);
        }
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
        else if (command instanceof BuyBehaviorCommand) {
            return validate((BuyBehaviorCommand) command);
        }
        else if (command instanceof AddToCartBehaviorCommand) {
            return validate((AddToCartBehaviorCommand) command);
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
        else if (command instanceof DeleteRatingCommand) {
            return toAvroEvent((DeleteRatingCommand) command);
        }
        else if (command instanceof BuyBehaviorCommand) {
            return toAvroEvent((BuyBehaviorCommand) command);
        }
        else if (command instanceof AddToCartBehaviorCommand) {
            return toAvroEvent((AddToCartBehaviorCommand) command);
        }

        return null;
    }

    private Command validate(PublishRatingCommand command) {
        boolean acceptable = (MIN_SCORE<=command.getScore() && command.getScore()<=MAX_SCORE) ? true: false;
        logger.info("PublishRatingCommand acceptable=" + acceptable);
        if (acceptable) {
            return command;
        }
        
        return new ErrorRatingCommand(command.getClientId(), command.getItemId(), "Unacceptable publish rating");
    }

    private Command validate(UpdateRatingCommand command) {
        boolean acceptable = (MIN_SCORE<=command.getScore() && command.getScore()<=MAX_SCORE) ? true: false;
        
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

    private Command validate(AddToCartBehaviorCommand command) {
        boolean acceptable = true;
        
        if (acceptable) {
            return command;
        }
        
        return new ErrorRatingCommand(command.getClientId(), command.getItemId(), "Unacceptable add to cart behavior");
    }

    private Command validate(BuyBehaviorCommand command) {
        boolean acceptable = true;
        
        if (acceptable) {
            return command;
        }
        
        return new ErrorRatingCommand(command.getClientId(), command.getItemId(), "Unacceptable buy behavior");
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

    private AvroEvent toAvroEvent(BuyBehaviorCommand command) {
        AvroBuyBehavior eventPayload = AvroBuyBehavior.newBuilder()
            .setClientId(command.getClientId())
            .setItemId(command.getItemId())
            .build();
        
        return wrap(eventPayload, avroBuyBehaviorEvent);
    }

    private AvroEvent toAvroEvent(AddToCartBehaviorCommand command) {
        AvroAddToCartBehavior eventPayload = AvroAddToCartBehavior.newBuilder()
            .setClientId(command.getClientId())
            .setItemId(command.getItemId())
            .build();
        
        return wrap(eventPayload, avroAddToCartBehaviorEvent);
    }

    private AvroEvent wrap(Object payload, String payloadType) {

        int partitionId = 1;
        if (payloadType.equals(avroUpdateRatingEvent) || 
            payloadType.equals(avroPublishRatingEvent) ||
            payloadType.equals(avroDeleteRatingEvent)) {
                partitionId = Integer.parseInt(partitionIdCommandRating);
        }
        else if (payloadType.equals(avroBuyBehaviorEvent)) {
            partitionId = Integer.parseInt(partitionIdBuyBehavior);
        }

        else if (payloadType.equals(avroAddToCartBehaviorEvent)) {
            partitionId = Integer.parseInt(partitionIdAddToCartBehavior);
        }

        return AvroEvent.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setPartitionId(partitionId)
                        .setTimestamp(Long.toString(System.currentTimeMillis()))
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
