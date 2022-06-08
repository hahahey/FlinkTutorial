package com.hahahey.apitest.processfunction;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author hahahey
 * @date 2022-05-30 23:52
 */
public class ProcessTest3_SideOutputCase {
    public static void main(String[] args)  throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //windows 下使用nc -lp 7777命令
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<SensorReading> mapDataStream = dataStreamSource.map((MapFunction<String, SensorReading>) s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });


        OutputTag<SensorReading> lowTemp = new OutputTag<SensorReading>("lowTemp"){};

        SingleOutputStreamOperator<SensorReading> singleOutputStreamOperator = mapDataStream
                .process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                if (value.getTemperature() > 30) {
                    out.collect(value);
                } else {
                    ctx.output(lowTemp, value);
                }
            }
        });

        singleOutputStreamOperator.print("highTemp");
        singleOutputStreamOperator.getSideOutput(lowTemp).print("lowTemp");


        env.execute();

    }
}
