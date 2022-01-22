package vn.datnguyen.recommender;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import vn.datnguyen.recommender.Service.WALPollingService;

import static java.lang.System.exit;

/**
 * Hello world!
 *
 */
@SpringBootApplication
public class App implements CommandLineRunner {

    @Autowired
    private WALPollingService walPollingService;

    public static void main( String[] args ) {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... args) {
        walPollingService.streamPhysicalWAL();
        exit(0);
    }
}
