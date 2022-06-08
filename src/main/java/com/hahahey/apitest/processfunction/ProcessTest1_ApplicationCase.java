package com.hahahey.apitest.processfunction;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author hahahey
 * @date 2022-05-30 23:52
 */
public class ProcessTest1_ApplicationCase {
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
                .process(new MyProcess(10))
                .print();

        env.execute();

    }

    private static class MyProcess  extends KeyedProcessFunction<Tuple,SensorReading,String>{

        //定义私有属性，当前统计的时间间隔
        private Integer interval;

        public MyProcess(int interval) {
            this.interval = interval;
        }

        //注册状态保存上一次的温度
        private ValueState<Double> lastTemp ;
        //注册定时器
        private ValueState<Long> timer;

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {


            //温度上升,注册定时器
            if(value.getTemperature() > lastTemp.value() && timer.value() == null){

                long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timer.update(ts);
            }

            //温度下降
            else if(value.getTemperature() < lastTemp.value() && timer.value() != null){
                //清空定时器
                ctx.timerService().deleteProcessingTimeTimer(timer.value());
                timer.clear();
            }
            //更新温度状态
            lastTemp.update(value.getTemperature());

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化state
            lastTemp=getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class,Double.MIN_VALUE));
            timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer",Long.class));
        }

        @Override
        public void close() throws Exception {
            lastTemp.clear();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Tuple currentKey = ctx.getCurrentKey();

            out.collect(currentKey.getField(0) + "连续" + interval + "s上升");
            timer.clear();
        }
    }
}
