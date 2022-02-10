package vn.datnguyen.recommender.Handler;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;

public interface Publisher {
    void execute(AvroEvent event);
}
