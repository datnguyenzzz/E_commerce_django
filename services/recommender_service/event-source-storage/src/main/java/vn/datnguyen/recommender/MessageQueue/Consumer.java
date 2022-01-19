package vn.datnguyen.recommender.MessageQueue;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;

public interface Consumer {
    void execute(AvroEvent event);
}
