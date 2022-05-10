package com.hahahey.apitest.transform;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hahahey
 * @date 2022-05-05 23:04
 */
public class TransformTest5_RichFunction {
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

        SingleOutputStreamOperator<Tuple2<String, Long>> map = dataStream.map(new RichMapFunction<SensorReading, Tuple2<String, Long>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化工作，一般是初始化状态 或者建立数据库连接
                System.out.println("open");
            }

            @Override
            public void close() throws Exception {
                //一般是关闭连接或清空状态
                System.out.println("close");
            }


            @Override
            public Tuple2<String, Long> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTimestamp());
            }
        });


        map.print();
        env.execute();

    }
}
