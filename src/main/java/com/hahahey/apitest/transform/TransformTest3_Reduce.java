package com.hahahey.apitest.transform;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hahahey
 * @date 2022-03-12 23:30
 */
public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> fileStream = env.readTextFile("src/main/resources/sensorreading.txt");

        SingleOutputStreamOperator<SensorReading> mapStream = fileStream.map((MapFunction<String, SensorReading>) s -> {
            String[] arr = s.split(",");
            return new SensorReading(arr[0],Long.parseLong(arr[1]),Double.parseDouble(arr[2]));
        });

        KeyedStream<SensorReading, Tuple> keyedStream = mapStream.keyBy("id");
        SingleOutputStreamOperator<SensorReading> reduceStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(
                        value1.getId(),
                        value2.getTimestamp(),
                        Math.max(value1.getTemperature(),value2.getTemperature())
                );
            }
        });


        reduceStream.print();
        env.execute();
    }
}
