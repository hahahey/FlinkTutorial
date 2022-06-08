package com.hahahey.apitest.table;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author hahahey
 * @date 2022-06-08 22:43
 */
public class TableTest01 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.读取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("src/main/resources/sensorreading.txt");

        //2.转换为对象
        SingleOutputStreamOperator<SensorReading> dataStream = dataStreamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //3.创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //4.基于流创建一张表
        Table table = tableEnvironment.fromDataStream(dataStream);

        //6.调用table api进行转换操作
        Table select = table.select("id,temperature")
                .where("id = '10001'");

        //7.执行sql

        //注册成表
        tableEnvironment.createTemporaryView("sensor",dataStream);
        //使用sql查询
        Table table1 = tableEnvironment.sqlQuery("select id,temperature from sensor where id = '10001'");

        DataStream<Row> rowDataStream = tableEnvironment.toAppendStream(select, Row.class);
        DataStream<Row> rowDataStream1 = tableEnvironment.toAppendStream(table1, Row.class);

        rowDataStream.print("table");
        rowDataStream1.print("sql");

        env.execute();
    }
}
