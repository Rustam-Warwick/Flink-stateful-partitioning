package StreamPartitioning.partitioners;

import StreamPartitioning.types.GraphQuery;
import StreamPartitioning.types.IncrementalPartitioner;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;

import java.util.Random;


public class TestPartitioner extends IncrementalPartitioner<String,Object> {
    Random random = new Random();
    public TestPartitioner(){
        super(String.class);

    }
    public TestPartitioner(Short K){
        super(String.class);
        this.K = K;
    }
    @Override
    public StatefulFunction functionOfType(FunctionType functionType) {
        return this;
    }


    @Override
    public String partition(Object scope) {
        // Random partitioning
        return String.valueOf(random.nextInt(this.K));
    }

    @Override
    public Object getUpdateRegion(GraphQuery input) {
        return null;
    }
}
