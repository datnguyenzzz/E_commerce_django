package vn.datnguyen.recommender.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CustomProperties {

    private static Properties props;
    private static CustomProperties INSTANCE;

    private CustomProperties () {}

    public static CustomProperties getInstance() {
        if (INSTANCE==null) {
            INSTANCE = new CustomProperties();
            InputStream inputStream = CustomProperties.class.getResourceAsStream("/application.properties");
            props = new Properties();
            try {
                props.load(inputStream);
                System.out.println(props.size());
            }
            catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }

        return INSTANCE;
    }

    public String getProp(String key) {
        return props.getProperty(key);
    }
}
