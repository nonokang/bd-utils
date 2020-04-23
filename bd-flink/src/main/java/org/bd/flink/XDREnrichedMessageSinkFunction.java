package org.bd.flink;

import lombok.Builder;
import lombok.NonNull;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.bd.flink.idgenerate.ESIdGenerator;
import org.bd.flink.idgenerate.IdGeneratorBuilder;
import org.bd.flink.idgenerate.IdGeneratorType;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

@Builder
public class XDREnrichedMessageSinkFunction implements ElasticsearchSinkFunction<XDREnrichedMessage> {
    @NonNull private String esIndex;
    @NonNull private IdGeneratorType idGeneratorType = IdGeneratorType.AUTO;

    public IndexRequest createIndexRequest(XDREnrichedMessage element) {
        final ObjectMapper mapper = new ObjectMapper();
        ESIdGenerator idGenerator = IdGeneratorBuilder.build(idGeneratorType);

        try {
            IndexRequest req = Requests.indexRequest()
                    .index(esIndex)
                    .type("_doc")
                    .source(mapper.writeValueAsString(element), XContentType.JSON);

            String id = idGenerator.generate(element.getTimestamp_event()
                    + element.getCell_id() + element.getApn(), 50);
            if (id != null) {
                return req.id(id);
            }

            return req;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void process(XDREnrichedMessage xdrEnrichedMessage, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        IndexRequest r = createIndexRequest(xdrEnrichedMessage);
        if (r != null) {
            requestIndexer.add(r);
        }
    }
}