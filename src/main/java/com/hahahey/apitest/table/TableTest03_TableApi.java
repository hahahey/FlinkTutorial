package com.hahahey.apitest.table;

import com.sun.prism.PixelFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author hahahey
 * @date 2022-06-14 23:17
 */
public class TableTest03_TableApi {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //读取文件
        tableEnvironment.connect(new FileSystem().path("src/main/resources/sensorreading.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("tempTable");


        Table tempTable = tableEnvironment.from("tempTable");
        //tableEnvironment.toAppendStream(tempTable, Row.class).print();

        //table api
        Table filterTable = tempTable.select("id , temp")
                .filter("id === '10004'")
                .groupBy("id")
                .select("id,id.count as cnt,temp.avg as avgTemp");

        // sql
        Table selectTable = tableEnvironment.sqlQuery("select id,count(id) as cnt,avg(temp) as avgTemp from tempTable where id = '10004' group by id");

        //打印输出
        tableEnvironment.toRetractStream(filterTable,Row.class).print("api");
        tableEnvironment.toRetractStream(selectTable,Row.class).print("sql");




        //输出到文件
        tableEnvironment.connect(new FileSystem().path("src/main/resources/out.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("outPutTable");

        tempTable.select("id,temp").insertInto("outPutTable");
        env.execute();
    }
}
