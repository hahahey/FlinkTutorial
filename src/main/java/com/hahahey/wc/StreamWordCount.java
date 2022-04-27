package com.hahahey.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author hahahey
 * @date 2022/1/3 18:35
 * @description:
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        //DataStreamSource<String> dataStreamSource = environment.readTextFile("src/main/resources/Hello.txt");
        //从socket流中读取数据


        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String ip = parameterTool.get("ip");
        String port = parameterTool.get("port");
        System.out.println(ip  + "  " + port );

        DataStreamSource<String> dataStreamSource = environment.socketTextStream(ip, Integer.parseInt(port));

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] arr = s.split(" ");
                for (String s1 : arr) {
                    collector.collect(new Tuple2<>(s1, 1));
                }
            }
        }).keyBy(0).sum(1);

        result.print();
        environment.execute();
    }
}
