package com.hahahey.apitest.source;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author hahahey
 * @date 2022-03-08 22:47
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 全局有序
        //env.setParallelism(1);

        DataStreamSource<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)));

        DataStreamSource<Integer> intDataStream = env.fromElements(1, 4, 3, 2, 5);

        dataStream.print("A");
        intDataStream.print("B");
        env.execute("CollectionSourceTest");

    }
}
