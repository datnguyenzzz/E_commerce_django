package vn.datnguyen.recommender.MessageQueue;

import vn.datnguyen.recommender.Domain.Event;

public interface Publisher {
    void execute(Event event);
}
