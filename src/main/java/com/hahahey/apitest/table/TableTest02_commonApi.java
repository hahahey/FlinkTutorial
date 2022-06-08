package com.hahahey.apitest.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author hahahey
 * @date 2022-06-08 23:14
 */
public class TableTest02_commonApi {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //1.1基于老版本planner的流处理
        EnvironmentSettings oldStreamSetting = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment oldStreamEnvironment = StreamTableEnvironment.create(env, oldStreamSetting);

        //1.2基于老版本planner的批处理
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment.create(environment);

        //1.3基于Blink的流处理
        EnvironmentSettings blinkStreamSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamEnvironment = StreamTableEnvironment.create(env, blinkStreamSetting);


        //1.4基于Blink的批处理
        EnvironmentSettings blinkBatchSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(blinkBatchSetting);
    }
}
