package org.bd.flink;

import lombok.Builder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

@Builder
public class JsonToXDRRawMessage implements MapFunction<String, XDRRawMessage> {
    private transient ObjectMapper mapper;

    @Override
    public XDRRawMessage map(String s) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        return mapper.readValue(s, XDRRawMessage.class);
    }

}
