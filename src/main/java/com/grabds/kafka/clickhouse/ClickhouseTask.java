package com.grabds.kafka.clickhouse;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.Collection;
import java.util.Map;

public class ClickhouseTask extends SinkTask{

    private Gson gson;

    @Override
    public String version() {
        return Constants.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        System.out.println("task is starting");

        gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
            .create();

    }

    @Override
    public void put(Collection<SinkRecord> records) {

        records.forEach(rec -> {
            Object val = rec.value();
            JsonObject recordAsJson = gson.fromJson(val.toString(), JsonObject.class);
            recordAsJson.entrySet().forEach(f -> {
                String key = f.getKey();
                String v = f.getValue().getAsString();
                String pair = String.format("%s = %s", key, v);
                System.out.println(pair);
            });

        });

    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        System.out.println("task is flushing");
    }

    @Override
    public void stop() {
        System.out.println("task is stopping");
    }
}
