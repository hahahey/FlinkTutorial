package com.hahahey.apitest.window;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author hahahey
 * @date 2022-05-11 23:35
 */
public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //windows 下使用nc -lp 7777命令
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<SensorReading> mapDataStream = dataStreamSource.map((MapFunction<String, SensorReading>) s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });


        //增量聚合
        SingleOutputStreamOperator<Integer> result = mapDataStream.keyBy("id")
                .timeWindow(Time.seconds(10))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer integer) {
                        return integer + 1;
                    }

                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer + acc1;
                    }
                });

        //result.print();




        //全窗口函数
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> fullWindowFunc = mapDataStream.keyBy("id")
                .timeWindow(Time.seconds(10))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        int count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id, window.getEnd(), count));
                    }
                });

        fullWindowFunc.print();


        OutputTag<SensorReading> late = new OutputTag<>("late");

        //其他可选API
        SingleOutputStreamOperator<SensorReading> singleOutputStreamOperator = mapDataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                //.trigger()
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .sum("temperature");


        //使用 SingleOutputStreamOperator 获取侧输出流并打印
        singleOutputStreamOperator.getSideOutput(late).print();
        env.execute();
    }
}
