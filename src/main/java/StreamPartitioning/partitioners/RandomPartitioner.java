package StreamPartitioning.partitioners;

import StreamPartitioning.types.GraphQuery;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import java.util.Random;


public class RandomPartitioner extends IncrementalPartitioner<String> {
    public Random random;

    public RandomPartitioner(){
        random = new Random();
    }
    public RandomPartitioner(Short K){
        this();
        this.PART_SIZE = K;
    }

    @Override
    public String partition(String scope) {
        // Random partitioning
        return String.valueOf(random.nextInt(this.PART_SIZE));
    }

    @Override
    public String getUpdateRegion(GraphQuery input) {
        return null;
    }
}
