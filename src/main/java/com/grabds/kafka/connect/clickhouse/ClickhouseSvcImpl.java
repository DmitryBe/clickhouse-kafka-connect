package com.grabds.kafka.connect.clickhouse;

import com.google.gson.*;
import org.apache.kafka.connect.errors.RetriableException;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.Tuple2;
import com.grabds.kafka.connect.exceptions.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClickhouseSvcImpl {

    private static Pattern clickhouseTypePattern = Pattern.compile("^Nullable\\(([\\d\\w]+)\\)");
    private static Logger log = LogManager.getLogger(ClickhouseSvcImpl.class);
    private ClickHouseDataSource dataSource;
    private String tableName;
    private List<ClickhouseTypeInfo> schema;
    private String insertSql;

    public List<ClickhouseTypeInfo> getSchema() {
        return schema;
    }

    public ClickhouseSvcImpl(String serverUri, String dbName, String table) throws SQLException {

        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setDatabase(dbName);

        this.dataSource = new ClickHouseDataSource(serverUri, properties);
        this.tableName = table;

        // retrieve schema info
        try(Connection connection = this.dataSource.getConnection()) {
            this.schema = this.getSchema(connection, table);
            this.insertSql = this.generateInsertSql();
        }

    }

    public String generateInsertSql() {

        String fieldsStr = schema.stream()
                .map(f -> f.fieldName)
                .collect(Collectors.joining(", "));

        String valsStr = IntStream.range(0, schema.size()).boxed()
                .collect(Collectors.toList())
                .stream()
                .map(x -> "?")
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s)", this.tableName, fieldsStr, valsStr);
    }

    public Integer batchProcess(Collection<JsonObject> recordsAsJson) {

        try(Connection connection = dataSource.getConnection()) {

            // create batch insert statment
            PreparedStatement statement = connection.prepareStatement(this.insertSql);
            populateInsertStatement(recordsAsJson, statement);
            return Arrays.stream(statement.executeBatch()).sum();

        } catch (SQLException e){
            // by throwing RetriableException we expect that Task will be restarted
            throw new RetriableException("error getting clickhouse connection", e);
        }
    }

    public List<ClickhouseTypeInfo> getSchema(Connection connection, String tableName) throws SQLException {
        /*
            get schema information for provided table
         */

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

    private void populateInsertStatement(Collection<JsonObject> recordsAsJson, PreparedStatement statement) {
        /*
            for every row (if pass schema validation) add into statement
         */

        recordsAsJson.stream().forEach(rec -> {

            try{

                prepInsertStatementForRec(rec).forEach(r -> {

                    try{
                        statement.setObject(r._1(), r._2());
                    }catch (SQLException e) {
                        log.error("statement setObject error", e);
                    }

                });
                statement.addBatch();

            }
            catch (MissedFieldException e){
                log.error("row schema validation failed", e);
            }
            catch (Exception e) {
                log.error("general error", e);
            }
        });
    }

    private Collection<Tuple2<Integer, Object>> prepInsertStatementForRec(JsonObject rec) throws RuntimeException {

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

    private ClickhouseTypeInfo extractTypeInfo(ResultSet rs, Integer idx) throws SQLException {
        /*
            gets clickhouse column info for current row (from rs),
            extracts column name and type (str)
            validates type for being Nullable
            returns ClickhouseTypeInfo(columnd_idx, name, type_name, isNullable)
        */

        String cName = rs.getString("name");
        String cType = rs.getString("type");

        Matcher m = clickhouseTypePattern.matcher(cType);
        if(m.matches()) {
            String typeName = m.group(1);
            return new ClickhouseTypeInfo(idx ,cName, typeName, true);
        }

        return new ClickhouseTypeInfo(idx, cName, cType, false);
    }

    private void logSchemaInfo(){
        log.info("schema information");
        this.schema.stream()
            .forEach(f -> {
                String msg = String.format("%s: %s %s [is nullable: $s]", f.idx, f.fieldName, f.strType, f.isNullable);
                log.info(msg);
            });
    }
}
