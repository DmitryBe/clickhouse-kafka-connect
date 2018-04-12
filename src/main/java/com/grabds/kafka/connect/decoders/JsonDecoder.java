package com.grabds.kafka.connect.decoders;

import com.google.gson.*;
import com.grabds.kafka.connect.clickhouse.ClickhouseSvcImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

public class JsonDecoder {

    private static Logger log = LogManager.getLogger(JsonDecoder.class);
    private Gson gson;

    public JsonDecoder() {

        gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
                .create();
    }

    public JsonObject decode(String str){
        return gson.fromJson(str, JsonObject.class);
    }

    public Collection<JsonObject> tryDecode(Collection<String> strList){
        /*
            return collection of successfully decoded records
         */

        Collection<JsonObject> jsonRecords = strList.stream()
            .map(r -> {

                Optional<JsonObject> oJson;
                try{
                    oJson = Optional.of(this.decode(r));
                }catch (JsonSyntaxException e){
                    String msg = String.format("incorrect json format for record: '%s'", r);
                    log.error(msg, e);
                    oJson = Optional.empty();
                }

                return oJson;
            })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        return jsonRecords;
    }
}
