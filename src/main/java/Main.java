import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("hello world!");
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Parallelism Factor
        System.out.println(env.getParallelism());

        //BUILDING INPUT
        long startTime = System.currentTimeMillis();
        Random rd = new Random();
        int nNodes = 10000000;
        List<double[]> paths = new ArrayList<>();
        for (int i = 0; i < nNodes; i++) {
            paths.add(new double[]{rd.nextDouble(), rd.nextDouble()});
        }
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(estimatedTime);

//        DataSource<double[]> text = env.fromCollection(paths);
        DataStream<double[]> text = env.fromCollection(paths);


//        DataSet<Integer> wordCounts = text
        DataStream<Integer> wordCounts = text
                .flatMap(new FlatMapFunction<double[], Integer>() {
                    @Override
                    public void flatMap(double[] line, Collector<Integer> out) throws Exception {
                            if (line[0] < 0.0)
                                out.collect(1);
                    }
                }
                );


//        System.out.println();
//        long startTime = System.currentTimeMillis();

        wordCounts.print();
        env.execute();

//        wordCounts.collect();

//        long estimatedTime = System.currentTimeMillis() - startTime;
//        System.out.println(estimatedTime);
    }
}
