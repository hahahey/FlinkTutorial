package com.hahahey.apitest.processfunction;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author hahahey
 * @date 2022-05-30 23:52
 */
public class ProcessTest2_KeyedProcessFunction {
    public static void main(String[] args)  throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //windows 下使用nc -lp 7777命令
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<SensorReading> mapDataStream = dataStreamSource.map((MapFunction<String, SensorReading>) s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });


        mapDataStream.keyBy("id")
                .process(new MyProcess())
                .print();

        env.execute();

    }

    private static class MyProcess extends KeyedProcessFunction<Tuple,SensorReading,Integer>{
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + "  定时器触发");
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
                out.collect(value.getId().length());

                //ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 10 ) * 1000);
                //ctx.timerService().deleteEventTimeTimer();
                //ctx.output();

                 ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
                //ctx.timerService().deleteProcessingTimeTimer();


        }
    }
}
