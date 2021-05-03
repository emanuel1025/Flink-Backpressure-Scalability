import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("hello world!");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Parallelism Factor
        System.out.println(env.getParallelism());


        //BUILDING INPUT
        long startTime = System.currentTimeMillis();
        Random rd = new Random();
        int nNodes = 25000000;
        List<double[]> paths = new ArrayList<>();
        for (int i = 0; i < nNodes; i++) {
            paths.add(new double[]{rd.nextDouble(), rd.nextDouble()});
        }
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(estimatedTime);

        DataStream<double[]> text = env.fromCollection(paths).assignTimestampsAndWatermarks(
                WatermarkStrategy
                .forBoundedOutOfOrderness(Duration.ofSeconds(1)));


        SingleOutputStreamOperator<String> wordCounts = text
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .process(new MyProcessWindowFunction());

        wordCounts.print();
        env.execute();
    }

    public static class MyProcessWindowFunction
            extends ProcessAllWindowFunction<double[], String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<double[]> elements, Collector<String> out) throws Exception {
            long count = 0;
            for (double[] in: elements) {
                count++;
            }
            out.collect("Window: " + context.window() + "count: " + count);
        }
    }
}
