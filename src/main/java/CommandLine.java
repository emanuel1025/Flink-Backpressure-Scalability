import com.beust.jcommander.Parameter;

public class CommandLine {

    @Parameter(names = "-numNodes", description = "Number of total nodes to send to Flink window function", required = false)
    public int numNodes = 25000000;

}
