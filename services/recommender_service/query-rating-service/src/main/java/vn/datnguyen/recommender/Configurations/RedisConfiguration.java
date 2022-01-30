package vn.datnguyen.recommender.Configurations;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;

import vn.datnguyen.recommender.Models.CachedQuery;

@Configuration
@EnableRedisRepositories(basePackageClasses = CachedQuery.class)
public class RedisConfiguration {

    @Value("${redis.host}")
    private String host;
    
    @Value("${redis.port}")
    private String port; 

    @Bean
    public JedisConnectionFactory jedisConnectionFactory() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();

        config.setHostName(host);
        config.setPort(Integer.parseInt(port));

        return new JedisConnectionFactory(config);
    }

    @Bean
    public RedisTemplate<Long, Object> redisTemplate() {
        RedisTemplate<Long, Object> template = new RedisTemplate<>();

        template.setConnectionFactory(jedisConnectionFactory());

        return template;
    }
}
