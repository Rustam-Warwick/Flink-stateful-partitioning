package StreamPartitioning.sources;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.UserQuery;
import StreamPartitioning.types.Vertex;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.function.IntConsumer;

/**
 * Responsible for fake graph generation.
 * Streaming Edges for a directed graph to be consumed by the main partitioner
 */
public class GraphGenerator extends RichParallelSourceFunction<UserQuery> {
    private Random random;
    private volatile boolean isRunning = true;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        random = new Random();
    }


    @Override
    public void run(SourceContext<UserQuery> ctx) throws Exception {
        random.ints(1000,0,1000).forEach(new IntConsumer() {
            String srcId = null;
            @Override
            public void accept(int value) {
                if(!isRunning)throw new NullPointerException(); // Running is stopped

                if(srcId==null){
                    srcId = String.valueOf(value); // Store the value wait for the next iteration
                }
                else{
                    // Add as the source
                    Vertex source = new Vertex().withId(srcId);
                    Vertex dest = new Vertex().withId(String.valueOf(value));
                    Edge edge = new Edge().betweenVertices(source,dest);
                    UserQuery query = new UserQuery(edge).changeOperation(UserQuery.OPERATORS.ADD);
                    ctx.collect(query);
                    this.srcId = null;
                }
            }
        });
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}

