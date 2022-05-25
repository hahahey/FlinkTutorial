package com.hahahey.apitest.state;

import akka.stream.impl.ReducerState;
import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author hahahey
 * @date 2022-05-25 23:16
 */
public class StateTest1_KeyedState {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置生成watermark的周期时间
        //env.getConfig().setAutoWatermarkInterval(100);

        //windows 下使用nc -lp 7777命令
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<SensorReading> mapDataStream = dataStreamSource.map((MapFunction<String, SensorReading>) s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });



        //自定义MapFunction
        SingleOutputStreamOperator<Integer> map = mapDataStream
                .keyBy("id")
                .map(new  MyKeyCountMapper());


        map.print();


        env.execute();
    }


    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {

        //声明一个键控状态
        private ValueState<Integer> keyCountState ;
        //其他类型的声明
        private ListState<String> listCountState;
        private MapState<String,Integer> mapCountState;
        private ReducingState<SensorReading> readingReducerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //声明一个键控状态
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count",Integer.class,0));
            listCountState = getRuntimeContext().getListState(new ListStateDescriptor<String>("list-count",String.class));
            mapCountState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("map-count",String.class,Integer.class));
            readingReducerState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("reduce-count", new ReduceFunction<SensorReading>() {
                @Override
                public SensorReading reduce(SensorReading t1, SensorReading t2) throws Exception {
                    return  new SensorReading(t1.getId(),1000,t1.getTemperature() + t2.getTemperature());
                }

            }, SensorReading.class));
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            //读取状态
            Integer count = keyCountState.value();
            count ++;
            //对状态赋值
            keyCountState.update(count);
            //清空状态
            //keyCountState.clear();

            listCountState.add(sensorReading.getId());

            for (String s : listCountState.get()) {
                System.out.println(s);
            }

            mapCountState.put(sensorReading.getId(), ThreadLocalRandom.current().nextInt(0,10));

            System.out.println(mapCountState.get(sensorReading.getId()));


            readingReducerState.add(sensorReading);
            System.out.println(readingReducerState.get());

            return count;
        }
    }

}
