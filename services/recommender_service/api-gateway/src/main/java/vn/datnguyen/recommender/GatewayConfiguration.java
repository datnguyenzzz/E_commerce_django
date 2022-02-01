package vn.datnguyen.recommender;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;

@Configuration
public class GatewayConfiguration {

    @Value("${services.queryRating}")
    private String QueryRatingService;

    @Value("${services.commandRating}")
    private String CommandRatingService;

    @Bean
    public RouteLocator gatewayRoutes(RouteLocatorBuilder builder) {
        return builder.routes()
            .route(r -> r.method(HttpMethod.GET)
                    .and()
                    .path("/rating")
                    .filters(
                        f -> f.rewritePath("/rating", "/api/v/1.0.0/rating")
                    )
                    .uri(QueryRatingService)
            )
            .route(r -> r.method(HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE)
                    .and()
                    .path("/rating")
                    .filters(
                        f -> f.rewritePath("/rating", "/api/v/1.0.0/rating")
                    )
                    .uri(CommandRatingService)
            )
            .build();
    }
}