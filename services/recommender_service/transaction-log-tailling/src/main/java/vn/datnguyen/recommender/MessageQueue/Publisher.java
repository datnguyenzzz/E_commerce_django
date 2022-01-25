package vn.datnguyen.recommender.MessageQueue;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;

public interface Publisher {
    public void execute(AvroEvent event);
}
