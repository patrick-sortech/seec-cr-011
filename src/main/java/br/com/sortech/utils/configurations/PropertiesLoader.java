package br.com.sortech.utils.configurations;

import com.sun.javafx.binding.StringFormatter;

import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;

public class PropertiesLoader {

    private Properties properties = new Properties();

    public PropertiesLoader(String propFileName) {
        this.properties = new Properties();

        try{
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream == null) {
                System.out.println(StringFormatter.format("Arquivo de propriedades: %s nao encontrado", propFileName));
            }

            this.properties.load(inputStream);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }



    public void listProperties() {

        this.properties.forEach((key, value) -> System.out.println("Key : " + key + ", Value : " + value));

        // Get all keys
        this.properties.keySet().forEach(x -> System.out.println(x));

        Set<Object> objects = this.properties.keySet();

            Enumeration e = this.properties.propertyNames();
            while (e.hasMoreElements()) {
                String key = (String) e.nextElement();
                String value = this.properties.getProperty(key);
                System.out.println("Key : " + key + ", Value : " + value);
            }


    }

}
