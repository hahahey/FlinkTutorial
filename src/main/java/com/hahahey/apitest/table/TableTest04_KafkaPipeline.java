package com.hahahey.apitest.table;

import org.apache.avro.data.Json;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author hahahey
 * @date 2022-06-15 22:14
 */
public class TableTest04_KafkaPipeline {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //连接kafka读取数据
        tableEnvironment.connect(new Kafka()
        .version("0.11")
        .topic("topic_01")
        .property("zookeeper.connect","localhost:2181")
        .property("bootstrap.servers","lolalhost:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("temestamp",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("kafka_inputtable");

        //简单转换
        Table kafkaInputtable = tableEnvironment.from("kafka_inputTable");
        Table selectTable = kafkaInputtable.select("id,temestamp")
                .filter("id === '10004'");

        //聚合统计
        Table aggTable = kafkaInputtable.groupBy("id")
                .select("id,id.count as cnt,temp.avg as avgTemp");

        //aggTable 不能写入

        //更新模式
        //对于流式查询，需要声明如何在表和外部连接器之间执行转换
        //与外部系统交换的消息类型，由更新模式(updateMode)指定
        //追加(Append)模式  表制作插入操作，和外部连机器只交换插入(insert)消息
        //撤回(retract)模式  表和外部连接器交换添加(Add)和撤回(Retract)消息
        //插入(insert)操作编码为Add消息，删除(delete)编码为Retract消息，更新(update)编码为上一条的Retract和吓一跳的Add消息
        //更新插入(Update)模式  更新和插入都被棉麻为Upsert消息，删除编码为Delete消息



//        tableEnvironment.connect(new Elasticsearch()
//        .version("6")
//        .host("localhost",9200,"http")
//        .index("sensor")
//        .documentType("temp"))
//                .inUpsertMode()
//                .withFormat(new Json())
//                .withSchema(new Schema()
//                .field("id",DataTypes.STRING())
//                .field("count",DataTypes.BIGINT()))
//                .createTemporaryTable("esOutputTable");
//
//        aggTable.insertInto("esOutputTable");

//        //输出到Mysql
//        String sinkDDL = "create table jdbcOutPutTable (id varchar(20) not null,cnt bigint not null ) " +
//                "with ('connecto.type' = 'idbc'," +
//                " 'connecto.type' = 'idbc',)" +
//                " 'connecto.url' = 'jdbc:mysql://localhost:3306/test'," +
//                " 'connecto.table' = 'seneor_count'," +
//                " 'connecto.driver' = 'com.mysql.jdbc.Driver'," +
//                " 'connecto.username' = 'root'," +
//                " 'connecto.password' = '123456' )";
//
//        tableEnvironment.sqlUpdate(sinkDDL); // 执行ddl创建表
//        aggTable.insertInto("jdbcOutPutTable");




        //连接kafka写入数据
        tableEnvironment.connect(new Kafka()
                .version("0.11")
                .topic("topic_02")
                .property("zookeeper.connect","localhost:2181")
                .property("bootstrap.servers","lolalhost:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temestamp",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("kafka_outputtable");

        selectTable.insertInto("kafka_outputtable");

        env.execute();
    }
}
