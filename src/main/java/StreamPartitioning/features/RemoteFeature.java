package StreamPartitioning.features;

import StreamPartitioning.types.GraphQuery;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.statefun.sdk.Context;
import java.util.concurrent.CompletableFuture;

public abstract class RemoteFeature<T> extends Feature<T> {
    public Integer lastSync = 0;
    public T value;
    public Short masterPart;
    public transient BaseReplicatedVertex vertex=null;
    public transient CompletableFuture<T> fuzzyValue=null;

    public RemoteFeature(String fieldName, T value, BaseReplicatedVertex v){
        super(fieldName);
        this.value = value;
        this.fuzzyValue = new CompletableFuture<>();
        this.fuzzyValue.complete(value);
        this.vertex = v;
    }
    public abstract boolean mergeValues(T elem);
    public void sync(Context context){
        return;
//          this.masterPart = this.vertex.getMasterPart();
//          this.attachedId = this.vertex.getId();
//          this.attachedToClass = this.vertex.getClass();
//          GraphQuery q = new GraphQuery(this).changeOperation(GraphQuery.OPERATORS.SYNC);
//          if(this.masterPart==null)this.vertex.sendThisToReplicas(context,this);
//          else this.vertex.sendThisToMaster(context,this);
    }

    @Override
    public void setValue(Feature<T> value,Context context) {
        if(value instanceof RemoteFeature){
            RemoteFeature<T> tmp = (RemoteFeature<T>) value;
            tmp.getValue().whenComplete((item,op)->{
                boolean hasChanged = this.mergeValues(item);
                if(hasChanged && tmp.masterPart!=null)this.sync(context);
            });
        }
    }

    @Override
    public CompletableFuture<T> getValue(){
        if(fuzzyValue!=null)return fuzzyValue;
        CompletableFuture<T> tmp = new CompletableFuture<>();
        tmp.complete(value);
        return tmp;
    }
}
