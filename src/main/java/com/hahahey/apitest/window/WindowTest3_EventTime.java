package com.hahahey.apitest.window;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author hahahey
 * @date 2022-05-24 22:44
 */
public class WindowTest3_EventTime {
    public static void main(String[] args) throws Exception {

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
        })

//        //基于升序数据设置时间戳和watermark
//      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//            @Override
//            public long extractAscendingTimestamp(SensorReading element) {
//                return element.getTimestamp() * 1000L;
//            }
//        });


                //乱序数据设置时间戳和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //基于事件时间的开窗聚合
        SingleOutputStreamOperator<SensorReading> result = mapDataStream.keyBy("id")
                .timeWindow(Time.seconds(4))
                .maxBy("temperature");


        result.print();
        env.execute();

    }
}
