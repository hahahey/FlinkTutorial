package com.hahahey.apitest.transform;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hahahey
 * @date 2022-05-09 22:12
 */
public class TransformTest6_partition {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> fileStream = env.readTextFile("src/main/resources/sensorreading.txt");


        SingleOutputStreamOperator<SensorReading> dataStream = fileStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        dataStream.print("input");

        // 1.shuffle
        //dataStream.shuffle().print("shuffle");

        // 2.keyby
        //dataStream.keyBy("id").print("keyBy");

        // 3.global
        dataStream.global().print("global");

        env.execute();
    }
}
