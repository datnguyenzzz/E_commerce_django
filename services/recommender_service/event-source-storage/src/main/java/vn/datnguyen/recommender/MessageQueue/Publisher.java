package vn.datnguyen.recommender.MessageQueue;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;

public interface Publisher {
    void execute(AvroEvent event);
}
