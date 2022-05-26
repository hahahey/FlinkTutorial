package com.hahahey.apitest.state;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @author hahahey
 * @date 2022-05-25 23:16
 */
public class StateTest2_OperatorState {
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


//        //根据传入的数据来记录状态
//        SingleOutputStreamOperator<Integer> map = mapDataStream.map(new MapFunction<SensorReading, Integer>() {
//            Integer count = 0;
//
//            @Override
//            public Integer map(SensorReading sensorReading) throws Exception {
//                return ++count;
//            }
//        });

        //自定义MapFunction
        SingleOutputStreamOperator<Integer> map = mapDataStream.map(new MyCountMapper());


        map.print();


        env.execute();
    }


    private static class MyCountMapper implements MapFunction<SensorReading,Integer>, ListCheckpointed<Integer>{
        //定义一个本地变量，作为算子状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            return ++count;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num : state) {
                 count += num;
            }
        }
    }
}
