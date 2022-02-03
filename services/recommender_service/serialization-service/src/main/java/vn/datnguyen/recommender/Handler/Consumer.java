package vn.datnguyen.recommender.Handler;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;

public interface Consumer {
    void execute(AvroEvent event);
}

