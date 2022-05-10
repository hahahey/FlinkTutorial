package com.hahahey.apitest.sink;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hahahey
 * @date 2022-05-09 23:38
 */
public class SinkTest3_elasticsearch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> fileStream = env.readTextFile("src/main/resources/sensorreading.txt");


        DataStream<SensorReading> dataStream = fileStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        HttpHost httpHost = new HttpHost("localhost", 9200);
        List<HttpHost> httpHosts = new ArrayList<>();


        //实现自定义es写入操作
        ElasticsearchSinkFunction elasticsearchSinkFunction = (ElasticsearchSinkFunction<SensorReading>) (data, runtimeContext, requestIndexer) -> {
            Map<String, String> map = new HashMap<>(4);
            map.put("id", data.getId());
            map.put("tem", String.valueOf(data.getTemperature()));

            //创建请求，作为向es发起的写入命令
            IndexRequest myindex = Requests.indexRequest()
                    .index("myindex")
                    .source(map);
            requestIndexer.add(myindex);

        };
        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, elasticsearchSinkFunction).build());

        env.execute();
    }
}
