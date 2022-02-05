package vn.datnguyen.recommender.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GetCustomProperties {

    private InputStream inputStream;
    private String value;

    public String getProp(String key) throws IOException {
        try {
            Properties props = new Properties();
            String propFile = "custom-config.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFile);

            if (inputStream != null) {
                props.load(inputStream);
            } else {
                throw new FileNotFoundException("config file not found");
            }

            value = props.getProperty(key);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
        finally {
            inputStream.close();
        }

        return value;
    }
}
