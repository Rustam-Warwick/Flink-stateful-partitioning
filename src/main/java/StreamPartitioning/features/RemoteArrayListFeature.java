package StreamPartitioning.features;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.statefun.sdk.Context;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class RemoteArrayListFeature extends RemoteFeature<ArrayList<Short>> {

    public RemoteArrayListFeature( BaseReplicatedVertex v, String fieldName){
        super(fieldName,new ArrayList<>(),v);
    }



    @Override
    public boolean mergeValues(ArrayList<Short> elem) {
        return false;
    }

    public void add( Short el, Context c){

        this.getValue().whenComplete((list,tr)->{
            if(!list.contains(el)){
                list.add(el);
                this.sync(c);
            }
        });
    }




}

