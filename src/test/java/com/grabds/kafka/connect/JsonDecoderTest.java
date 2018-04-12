package com.grabds.kafka.connect;

import static org.junit.Assert.assertTrue;

import com.google.gson.*;
import com.grabds.kafka.connect.decoders.JsonDecoder;
import org.junit.Test;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

public class JsonDecoderTest {

    @Test
    public void testJsonDecode() throws Exception {

        JsonDecoder decoder = new JsonDecoder();

        String recordStr = "{\"name\": \"user100\"}";
        JsonObject json = decoder.decode(recordStr);
        assertTrue(json != null);
    }

    @Test
    public void testJsonDecodeList() throws Exception {
        JsonDecoder decoder = new JsonDecoder();

        Collection<String> records = Arrays.asList(
                "{\"name user100\"}",
                "{\"name\": \"user100\"}"
        );

        Collection<JsonObject> jsonRecords = decoder.tryDecode(records);

        // one record has incorrect format
        assertTrue(jsonRecords.size() == 1);
    }
}
