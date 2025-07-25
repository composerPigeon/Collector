package cz.cuni.matfyz.collector.server;
import java.util.Arrays;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry class of spring boot application
 */
@SpringBootApplication
@ConfigurationPropertiesScan("cz.cuni.matfyz.collector.server.configurationproperties")
@EnableScheduling
public class Application {

    private static final Logger _logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        Environment env = SpringApplication.run(Application.class, args).getEnvironment();
        _logger.info("Server is running on port: {}", env.getProperty("local.server.port"));
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {

            _logger.info("Let's inspect the beans provided by Spring Boot:");

            String[] beanNames = ctx.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String beanName : beanNames) {
                System.out.println(beanName);
            }

        };
    }

}
