package com.grabds.kafka.clickhouse;

import com.google.gson.*;
import com.sun.deploy.util.StringUtils;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.Tuple2;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class ClickhouseTypeInfo
{
    public Integer idx;
    public String fieldName;
    public String strType;
    public Boolean isNullable;

    public ClickhouseTypeInfo(Integer idx, String fieldName, String strType, Boolean isNullable) {
        this.idx = idx;
        this.fieldName = fieldName;
        this.strType = strType;
        this.isNullable = isNullable;
    }
}

class MissedFieldException extends RuntimeException{

    public MissedFieldException(String message) {
        super(message);
    }
}

public class ClickhouseSvcImpl {

    private static Pattern clickhouseTypePattern = Pattern.compile("^Nullable\\(([\\d\\w]+)\\)");
    private static Logger log = LogManager.getLogger(ClickhouseSvcImpl.class);
    private ClickHouseDataSource dataSource;
    private String tableName;
    private List<ClickhouseTypeInfo> schema;

    public List<ClickhouseTypeInfo> getSchema() {
        return schema;
    }

    public ClickhouseSvcImpl(String serverUri, String dbName, String table) throws Exception {

        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setDatabase(dbName);

        this.dataSource = new ClickHouseDataSource(serverUri, properties);
        this.tableName = table;

        // retrieve schema info
        try(Connection connection = this.dataSource.getConnection()) {
            this.schema = this.getSchema(connection, table);
        }

    }

    public String generateInsertSql() {

        List<String> fields = schema.stream().map(f -> f.fieldName).collect(Collectors.toList());
        List<String> vals = IntStream.range(0, schema.size()).boxed()
                .collect(Collectors.toList())
                .stream()
                .map(x -> "?")
                .collect(Collectors.toList());

        return String.format("INSERT INTO %s (%s) VALUES (%s)", this.tableName, StringUtils.join(fields, ", "),  StringUtils.join(vals, ", "));
    }

    public Integer batchProcess(Collection<JsonObject> recordsAsJson) throws Exception {

        try(Connection connection = dataSource.getConnection()) {

            // create batch insert statment
            PreparedStatement statement = connection.prepareStatement(this.generateInsertSql());

            // process records
            recordsAsJson.stream().forEach(rec -> {

                try{

                    prepStatementRec(rec).forEach(r -> {
                        try{
                            statement.setObject(r._1(), r._2());
                        }catch (SQLException e) {
                            log.error("statement setObject error", e);
                        }
                    });
                    statement.addBatch();

                }
                catch (MissedFieldException e){
                    log.error("missed field exception", e);
                }
                catch (Exception e) {
                    log.error("general error", e);
                }
            });

            Integer processed = Arrays.stream(statement.executeBatch()).sum();

            return processed;
        }
    }

    private Collection<Tuple2<Integer, Object>> prepStatementRec(JsonObject rec) throws RuntimeException {

        return schema.stream().map(field -> {

            JsonElement jsonElem = rec.get(field.fieldName);
            if(jsonElem == null && !field.isNullable) {
                String msg = String.format("rec: '%s' doesn't contain required field: '%s'", rec.toString(), field.fieldName);
                throw new MissedFieldException(msg);
            } else {
                Object val = null;
                if(jsonElem != null)
                    val = jsonElem.getAsString();
                return new Tuple2<Integer, Object>(field.idx, val);
            }

        }).collect(Collectors.toList());
    }

    public List<ClickhouseTypeInfo> getSchema(Connection connection, String tableName) throws Exception {

        String sql = String.format("DESCRIBE %s", tableName);
        PreparedStatement schemaStmnt = connection.prepareStatement(sql);
        ResultSet rs = schemaStmnt.executeQuery();

        List<ClickhouseTypeInfo> schema = new ArrayList<ClickhouseTypeInfo>();
        Integer counter = 1;
        while (rs.next()){
            ClickhouseTypeInfo typeInfo = extractTypeInfo(rs, counter);
            schema.add(typeInfo);
            counter += 1;
        }

        return schema;
    }

    private ClickhouseTypeInfo extractTypeInfo(ResultSet rs, Integer idx) throws Exception{

        String cName = rs.getString("name");
        String cType = rs.getString("type");

        Matcher m = clickhouseTypePattern.matcher(cType);
        if(m.matches()) {
            String typeName = m.group(1);
            return new ClickhouseTypeInfo(idx ,cName, typeName, true);
        }

        return new ClickhouseTypeInfo(idx, cName, cType, false);
    }
}
