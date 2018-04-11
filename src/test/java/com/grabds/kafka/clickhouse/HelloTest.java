package com.grabds.kafka.clickhouse;

import static org.junit.Assert.assertTrue;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.junit.Test;
import java.util.*;
import java.util.stream.Collectors;

public class HelloTest {

    @Test
    public void testJsonDeser() throws Exception {

        Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
            .create();

        List<String> records = Arrays.asList(
                "{\"UpdateDate\": \"2017-11-08\", \"GeoHash\":\"geo01\", \"NBooked\":\"1\"}",
                "{\"UpdateDate\": \"2017-11-08\", \"GeoHash\":\"geo01\", \"NBooked\":\"1\"}"
        );

        List<JsonObject> jsonList = records.stream().map(rec -> gson.fromJson(rec, JsonObject.class)).collect(Collectors.toList());

        jsonList.stream().forEach(rec -> {
            List<String> filedsName = rec.entrySet().stream().map(f -> f.getKey()).collect(Collectors.toList());

        });

        assertTrue(true);
    }

    @Test
    public void testMessage() {

        String str1 = String.format("%s = %d", "1", 1);

        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
                .create();

        String recordStr = "{\"name\": \"user100\"}";
        JsonObject recordAsJson = gson.fromJson(recordStr, JsonObject.class);

        Object o = recordAsJson.get("name");
        Object o1 = recordAsJson.get("name1");

        List<String> messages = Arrays.asList("Hello", "World!", "How", "Are", "You");
        List<String> r = messages.stream()
                .map(x -> x.toUpperCase())
                .collect(Collectors.toList());

        recordAsJson.entrySet().forEach(f -> {
            String key = f.getKey();
            String val = f.getValue().getAsString();
            String pair = String.format("%s = %s", key, val);
            System.out.println(pair);
        });

        assertTrue(true);
    }

}
