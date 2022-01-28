package vn.datnguyen.recommender.Configurations;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.SocketUtils;

@Configuration
public class RandomPortConfiguration implements WebServerFactoryCustomizer<ConfigurableServletWebServerFactory> {
    
    @Value("${port.number.min:7000}")
    private Integer minPort; 

    @Value("${port.number.max:9000}")
    private Integer maxPort;

    public void customize(ConfigurableServletWebServerFactory factory) {
        int port = SocketUtils.findAvailableUdpPort(minPort, maxPort);
        factory.setPort(port);
        System.getProperties().put("server.port", port);
    }
}
