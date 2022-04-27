package com.hahahey.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hahahey
 * @date 2022-03-08 23:04
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> fileDataStream = env.readTextFile("src/main/resources/Hello.txt");
        fileDataStream.print();

        env.execute("SourceTest2_File");
    }
}
