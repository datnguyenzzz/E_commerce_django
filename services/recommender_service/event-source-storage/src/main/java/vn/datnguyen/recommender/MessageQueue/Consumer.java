package vn.datnguyen.recommender.MessageQueue;

import vn.datnguyen.recommender.Serialization.AvroEvent;

public interface Consumer {
    void execute(AvroEvent event);
}
