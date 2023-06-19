package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.DataFinishTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class StreamMapPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> source = executionEnvironment.fromCollection(new OneThousandSource(), String.class);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = source.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(String.valueOf(getRuntimeContext().getIndexOfThisSubtask()), Integer.valueOf(s));
            }
        });
        mapStream.setParallelism(10);
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapStream.keyBy((KeySelector<Tuple2<String, Integer>, String>) element -> element.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> apply = keyedStream
                .window(GlobalWindows.create())
                .trigger(DataFinishTrigger.create())
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, GlobalWindow>() {
                    @Override
                    public void apply(String s, GlobalWindow window, Iterable<Tuple2<String, Integer>> inputData, Collector<Tuple2<String, Integer>> out) {
                        Set<String> uniqueStringKeys = new HashSet<>();
                        int sum = 0;
                        for (Tuple2<String, Integer> data : inputData) {
                            uniqueStringKeys.add(data.f0);
                            sum += data.f1;
                        }
                        String key = uniqueStringKeys.size() + " " + uniqueStringKeys;
                        out.collect(Tuple2.of(key, sum));
                    }
                });
        apply.setParallelism(20);
        apply.print();
        CloseableIterator<Tuple2<String, Integer>> jobResult = apply.executeAndCollect();
        while (jobResult.hasNext())
            System.out.println(jobResult.next());
    }
}
