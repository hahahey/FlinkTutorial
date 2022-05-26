package com.hahahey.apitest.state;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.beans.Transient;

/**
 * @author hahahey
 * @date 2022-05-25 23:16
 */
public class StateTest3_KeyedStateApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //windows 下使用nc -lp 7777命令
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<SensorReading> mapDataStream = dataStreamSource.map((MapFunction<String, SensorReading>) s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> flatMapDs = mapDataStream.keyBy("id")
                .flatMap(new MyFlatMapFunction(10.0));

        flatMapDs.print();
        env.execute();

    }

    private static class MyFlatMapFunction extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        //温度跳变阈值
        private  final  Double threshold;

        //定义状态，保存上一次的温度值

        private   ValueState<Double> valueState;

        public MyFlatMapFunction(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("value", Double.class));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {


            if (valueState.value() != null) {
                if (Math.abs((sensorReading.getTemperature() - valueState.value())) > threshold) {
                    collector.collect(new Tuple3<>(sensorReading.getId(), valueState.value(), sensorReading.getTemperature()));
                }
            }

            valueState.update(sensorReading.getTemperature());

        }

        @Override
        public void close() throws Exception {
            valueState.clear();
        }
    }
}
