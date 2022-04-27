package com.hahahey.apitest.transform;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import scala.Tuple2;

import java.util.Collections;

/**
 * @author hahahey
 * @date 2022-03-13 23:07
 */
public class TransformTest4_MultipleStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> fileStream = env.readTextFile("src/main/resources/sensorreading.txt");

        SingleOutputStreamOperator<SensorReading> mapDataStream = fileStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        SplitStream<SensorReading> splitDataStream = mapDataStream.split((OutputSelector<SensorReading>) value -> (value.getTemperature() > 37) ? Collections.singleton("high") : Collections.singleton("low"));

        DataStream<SensorReading> high = splitDataStream.select("high");
        DataStream<SensorReading> low = splitDataStream.select("low");

        ConnectedStreams<SensorReading, SensorReading> connect = high.connect(low);
        SingleOutputStreamOperator<Tuple2> map = connect.map(new CoMapFunction<SensorReading, SensorReading, Tuple2>() {
            @Override
            public Tuple2 map1(SensorReading value) throws Exception {
                return new Tuple2(value.getId(), value.getTemperature());
            }

            @Override
            public Tuple2 map2(SensorReading value) throws Exception {
                return new Tuple2(value.getId(), value.getTemperature());
            }
        });

        high.print();
        low.print();
        map.print();

        env.execute();


    }
}
