package vn.datnguyen.recommender.MessageQueue;

import vn.datnguyen.recommender.Domain.Event;

public interface Consumer {
    void execute(Event event);
}
