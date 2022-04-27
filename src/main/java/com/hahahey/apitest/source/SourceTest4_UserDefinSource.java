package com.hahahey.apitest.source;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author hahahey
 * @date 2022-03-09 0:02
 */
public class SourceTest4_UserDefinSource {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> userDefineSourceDataStream = env.addSource(new SourceFunction<SensorReading>() {

            boolean flag = true;



            @Override
            public void run(SourceContext<SensorReading> sourceContext) throws Exception {

                Random random = new Random();

                Map<String,Double> sensorMap = new HashMap<>(10);

                for(int i = 0;i<10;i++){
                    sensorMap.put("sensor_" + i, 60 + random.nextGaussian() * 20);
                }

//                while (flag){
//                    for (String s : sensorMap.keySet()) {
//                    sourceContext.collect(new SensorReading(s,System.currentTimeMillis(),sensorMap.get(s) + random.nextGaussian() * 10));
//                    }
//                    TimeUnit.SECONDS.sleep(2);
//                }


                while(flag){
                    System.out.println("start------------------");

                        sourceContext.collect(new SensorReading("sensor_1",1111111,12.345));


                    System.out.println("end  -------------- ");

                    TimeUnit.SECONDS.sleep(2);
                }

            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        userDefineSourceDataStream.print();
        env.execute("SourceTest4_UserDefinSource");
    }
}
