package StreamPartitioning.sources;

import StreamPartitioning.edges.Edge;
import StreamPartitioning.types.GraphQuery;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.function.IntConsumer;

/**
 * Responsible for fake graph generation.
 * Streaming Edges for a directed graph to be consumed by the main partitioner
 */
public class GraphGenerator extends RichParallelSourceFunction<GraphQuery> {
    private Random random;
    private volatile boolean isRunning = true;
    private int N; // Num of vertices
    private int D; // Average degree(in+out) per vertex
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        random = new Random();
        this.N = 5;
        this.D  = 2;

    }

    public boolean streamVertex(short value,double p){
        if(value>0){
            double coin = Math.random();
            if(coin <=p){
                return true;
            }
        }
        return false;
    }
    @Override
    public void run(SourceContext<GraphQuery> ctx) throws Exception {
        short edges[] = new short[N];
        double pVertexStream = (double) 1/D;
        random.ints(N*D,0,N).forEach(new IntConsumer() {
            Integer srcId = null;
            @Override
            public void accept(int value) {
                if(!isRunning)throw new NullPointerException(); // Running is stopped

                if(srcId==null){
                    srcId = value; // Store the value wait for the next iteration
                }
                else{
                    // 1. Add as the source

                    BaseReplicatedVertex src = new BaseReplicatedVertex(String.valueOf(srcId));
                    BaseReplicatedVertex dest = new BaseReplicatedVertex(String.valueOf(value));
                    Edge edge = new Edge(src,dest);
                    GraphQuery query = new GraphQuery(edge).changeOperation(GraphQuery.OPERATORS.ADD);
                    ctx.collect(query);
                    // 2. Increment data structure
                    edges[value]++;
                    edges[srcId]++;

                    // 3. Decide if Vertex should be streamed as well
//                    if(streamVertex(edges[value],pVertexStream)){
//                        edges[value] = Short.MIN_VALUE;
//                        Vertex v = new Vertex().withId(String.valueOf(value));
//                        UserQuery qvertex = new UserQuery(v).changeOperation(UserQuery.OPERATORS.ADD);
//                        ctx.collect(qvertex);
//                    }
//                    if(streamVertex(edges[srcId],pVertexStream)){
//                        edges[srcId] = Short.MIN_VALUE;
//                        Vertex v = new Vertex().withId(String.valueOf(srcId));
//                        UserQuery qvertex = new UserQuery(v).changeOperation(UserQuery.OPERATORS.ADD);
//                        ctx.collect(qvertex);
//                    }
                    //
                    this.srcId = null;
                }
            }
        });

        while(isRunning){
            // This part is needed since the jobs will close once this source function returns
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}

