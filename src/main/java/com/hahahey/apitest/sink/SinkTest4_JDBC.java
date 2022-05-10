package com.hahahey.apitest.sink;

import com.hahahey.apitest.beans.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author hahahey
 * @date 2022-05-10 23:22
 */
public class SinkTest4_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> fileStream = env.readTextFile("src/main/resources/sensorreading.txt");


        DataStream<SensorReading> dataStream = fileStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        dataStream.addSink(new MyJdbcSink());
        env.execute();

    }

    private static class MyJdbcSink extends RichSinkFunction<SensorReading> {

        Connection connection = null;
        PreparedStatement insertPre = null;
        PreparedStatement updatePre = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306", "username", "password");
            insertPre = connection.prepareStatement("insert into table (id,temp) values(?,?)");
            updatePre = connection.prepareStatement("update table set temp = ? where id = ?");
        }

        @Override
        public void close() throws Exception {
            updatePre.close();
            insertPre.close();
            connection.close();
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updatePre.setDouble(1, value.getTemperature());
            updatePre.setString(2, value.getId());
            updatePre.execute();
            if (updatePre.getUpdateCount() == 0) {
                insertPre.setDouble(1, value.getTemperature());
                insertPre.setString(2, value.getId());
            }
        }
    }

}
