package com.atguigu.chapter01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount_Batch {
    public static void main(String[] args) {
        ExecutionEnvironment evn = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lineDs = evn.readTextFile("input/word.txt");
        lineDs.flatMap((String line, Collector< Tuple2<String,Long> > out)->
                {
                    String[] split = line.split(" ");
                    for (String word : split) {
                        out.collect(Tuple2.of(word,1L));
                    }
                }
                ).returns(Types.TUPLE(Types.STRING,Types.LONG));
    }
}
