package com.grabds.kafka.connect;

import com.google.gson.*;
import com.grabds.kafka.connect.clickhouse.ClickhouseSvcImpl;
import com.grabds.kafka.connect.decoders.JsonDecoder;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class ClickhouseTask extends SinkTask{

    private static Logger log = LogManager.getLogger(ClickhouseTask.class);
    private JsonDecoder decoder;
    private ClickhouseSvcImpl svc;

    @Override
    public String version() {
        return Constants.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {

        log.info("ClickhouseTask is starting");

        // init json decoder
        decoder = new JsonDecoder();

        // init clickhouse service
        try {
            String clickhouseUri = props.get(Constants.CLICKHOUSE_URI);
            String clickhouseDb = props.get(Constants.CLICKHOUSE_DB);
            String clickhouseTable = props.get(Constants.CLICKHOUSE_TABLE);
            svc = new ClickhouseSvcImpl(clickhouseUri, clickhouseDb, clickhouseTable);
            log.info("clickhouse svc created");
        }catch (SQLException e){
            log.error("error initializing clickhouse service", e);
            throw new org.apache.kafka.connect.errors.RetriableException(e);
        }
        catch (Exception e){
            log.error("error initializing clickhouse service", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {

        // expected SinkRecord type is String

        Collection<String> strRecords = records.stream().map(r -> {
            return r.value().toString();
        }).collect(Collectors.toList());

        // str -> jsonObject
        Collection<JsonObject> jsonRecords = decoder.tryDecode(strRecords);

        Integer processedCount = this.svc.batchProcess(jsonRecords);
        log.info(String.format("processed: {}", processedCount));
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {}

    @Override
    public void stop() {
        log.info("ClickhouseTask is stopping");
    }
}
