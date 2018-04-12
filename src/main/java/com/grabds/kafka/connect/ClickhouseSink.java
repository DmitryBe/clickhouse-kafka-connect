package com.grabds.kafka.connect;

import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;

public class ClickhouseSink extends SinkConnector{

    private String clickhouseUri;
    private String clickhouseDb;
    private String clickhouseTable;

    @Override
    public String version() {
        return Constants.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        clickhouseUri = props.get(Constants.CLICKHOUSE_URI);
        clickhouseDb = props.get(Constants.CLICKHOUSE_DB);
        clickhouseTable = props.get(Constants.CLICKHOUSE_TABLE);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ClickhouseTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {

        final List<Map<String, String>> configs = new LinkedList<>();

        for (int i = 0; i < maxTasks; i++) {
            final Map<String, String> config = new HashMap<>();
            config.put(Constants.CLICKHOUSE_URI, clickhouseUri);
            config.put(Constants.CLICKHOUSE_DB, clickhouseDb);
            config.put(Constants.CLICKHOUSE_TABLE, clickhouseTable);

            configs.add(config);
        }

        return configs;

    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {

        final ConfigDef configDef = new ConfigDef();
        configDef.define(Constants.CLICKHOUSE_URI, Type.STRING, "jdbc:clickhouse://localhost:8123", Importance.HIGH, "Clickhouse uri (jdbc:clickhouse://<host>:<port>)");
        configDef.define(Constants.CLICKHOUSE_DB, Type.STRING, Importance.HIGH, "Database name");
        configDef.define(Constants.CLICKHOUSE_TABLE, Type.STRING, Importance.HIGH, "Table name");

        return configDef;
    }
}
