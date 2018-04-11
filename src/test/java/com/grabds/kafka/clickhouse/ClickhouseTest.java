package com.grabds.kafka.clickhouse;

import com.google.common.collect.Range;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;


public class ClickhouseTest {

    String serverUri = "internal-af67bdeb33cac11e8a4310671e315a7c-1771144458.ap-southeast-1.elb.amazonaws.com";
    Integer serverPort = 8123;
    String url = String.format("jdbc:clickhouse://%s:%s", serverUri, serverPort);
    String dbName = "DB01";


    @Test
    public void clickhouseSvc() throws Exception {

        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
                .create();

        List<String> records = Arrays.asList(
                "{\"UpdateDate\": \"2017-11-08\", \"GeoHash\":\"geo1000\", \"NBooked\":\"101\", \"N1\": 10 }"
        );

        List<JsonObject> jsonRecs = records.stream().map(rec -> gson.fromJson(rec, JsonObject.class)).collect(Collectors.toList());


        ClickhouseSvcImpl svc = new ClickhouseSvcImpl(url, dbName, "Table2");
        Integer r = svc.batchProcess(jsonRecs);

        assertTrue(true);
    }

    @Test
    public void testInsert() throws Exception {

        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setDatabase(dbName);

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        Connection connection = dataSource.getConnection();

        PreparedStatement statement = connection.prepareStatement("INSERT INTO Table1_c (UpdateDate, GeoHash, NBooked) VALUES (?, ?, ?)");

        statement.setObject(1, Date.valueOf("2017-11-08"));
        statement.setObject(2, "geo200");
        statement.setObject(3, 21);
        statement.addBatch();

        int[] r = statement.executeBatch();

        assertTrue(true);
    }

}
