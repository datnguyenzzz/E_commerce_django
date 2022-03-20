package vn.datnguyen.recommender.Handler;

import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;

@Service
public class ReplyingKafkaService {
    public ReplyingKafkaService() {}

    public Object requestThenReply(AvroEvent event) {
        return null;
    }
}
