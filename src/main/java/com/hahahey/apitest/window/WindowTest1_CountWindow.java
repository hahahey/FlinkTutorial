package com.hahahey.apitest.window;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hahahey
 * @date 2022-05-11 23:35
 */
public class WindowTest1_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //windows 下使用nc -lp 7777命令
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<SensorReading> mapDataStream = dataStreamSource.map((MapFunction<String, SensorReading>) s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        SingleOutputStreamOperator<Object> coutnWindowSream = mapDataStream.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Object>() {
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<>(0.0, 0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(SensorReading sensorReading, Tuple2<Double, Integer> doubleIntegerTuple2) {
                        return new Tuple2<>(doubleIntegerTuple2.f0 + sensorReading.getTemperature(), doubleIntegerTuple2.f1 + 1);
                    }

                    @Override
                    public Object getResult(Tuple2<Double, Integer> doubleIntegerTuple2) {
                        return doubleIntegerTuple2.f0 / doubleIntegerTuple2.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> doubleIntegerTuple2, Tuple2<Double, Integer> acc1) {
                        return new Tuple2<>(doubleIntegerTuple2.f0 + acc1.f0, doubleIntegerTuple2.f1 + acc1.f1);
                    }
                });

        coutnWindowSream.print();



        env.execute();
    }
}
