package com.hahahey.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @author hahahey
 * @date 2022/1/3 17:25
 * @description:
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();

        DataSource<String> stringDataSource = environment.readTextFile("src/main/resources/Hello.txt");

        FlatMapOperator<String, Tuple2<String, Integer>> stringTuple2FlatMapOperator = stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] arr = s.split(" ");
                for (String s1 : arr) {
                    collector.collect(new Tuple2<>(s1, 1));
                }
            }
        });
        AggregateOperator<Tuple2<String, Integer>> result = stringTuple2FlatMapOperator.groupBy(0).sum(1);
        result.print();

    }
}
