import com.beust.jcommander.JCommander;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Main {
    public static void main(String[] args) throws Exception {

        CommandLine cli = new CommandLine();
        JCommander cmdr = new JCommander(cli, args);
        cmdr.usage();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Parallelism Factor
        System.out.println("Parallelism Factor is " + env.getParallelism());

        //Building Input
        long startTime = System.currentTimeMillis();
        Random rd = new Random();
        int nNodes = cli.numNodes;

        //Number of nodes
        System.out.println("Number of nodes generated is " + nNodes);
        List<double[]> paths = new ArrayList<>();
        for (int i = 0; i < nNodes; i++) {
            paths.add(new double[]{rd.nextDouble(), rd.nextDouble()});
        }
        long estimatedTime = System.currentTimeMillis() - startTime;

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
