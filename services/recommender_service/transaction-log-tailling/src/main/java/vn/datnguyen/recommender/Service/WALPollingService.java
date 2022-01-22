package vn.datnguyen.recommender.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class WALPollingService {

    private final Logger logger = LoggerFactory.getLogger(WALPollingService.class);

    public WALPollingService() {}

    public void streamPhysicalWAL() {
        while (true) {
            logger.info("TRANSACTION-LOG-TAILING: Start polling from WAL");
        }
    }
}
