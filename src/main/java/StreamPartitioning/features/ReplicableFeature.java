package StreamPartitioning.features;

import StreamPartitioning.types.GraphQuery;
import StreamPartitioning.types.Identifiers;
import StreamPartitioning.types.ReplicableGraphElement;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.statefun.sdk.Context;
import java.util.concurrent.CompletableFuture;

public abstract class ReplicableFeature<T> extends Feature<T> {
    public Integer lastSync = -1;
    public final T value;
    public ReplicableGraphElement.STATE replicationState = ReplicableGraphElement.STATE.NONE;
    public Short part = null;
    public transient ReplicableGraphElement element=null;
    public transient CompletableFuture<T> fuzzyValue=null;
    public ReplicableFeature(){
        super();
        this.value = null;
        this.lastSync = -1;
        this.element=null;
        this.fuzzyValue = new CompletableFuture<>() ;
        this.part = null;
    }
    public ReplicableFeature(String fieldName, T value, ReplicableGraphElement el){
        super(fieldName,el);
        this.value = value;
        this.fuzzyValue = null;
        this.element = el;
        this.lastSync = -1;
        this.replicationState = el.getState();
        this.part = el.getPart();
    }
    public abstract boolean handleNewReplica(T elem);
    abstract public void replaceValue(T elem);

    public void updateFuzzyValue(){
        if(this.fuzzyValue!=null && !this.fuzzyValue.isDone())this.fuzzyValue.complete(this.value);
        else{
            this.fuzzyValue = new CompletableFuture<T>();
            this.fuzzyValue.complete(this.value);
        }
    }

    /**
     * Handles direct changes of the value. Direct changes of replicas have to be waited until master approves!
     * Be very careful with this, do not call if the state is not changed
     * @param context
     */
    public void sync(Context context,boolean waitForMasterApproval){
        // Value explicitly changed need to sync
        if(this.replicationState== ReplicableGraphElement.STATE.MASTER){
            // 1. If master simply notify the replicas about this value
            this.lastSync++;
            this.updateFuzzyValue();
            this.element.sendMessageToReplicas(context,ReplicableFeature.prepareMessage(this));
        }
        else if(this.replicationState==ReplicableGraphElement.STATE.REPLICA){
            // 2. If replica
            if(waitForMasterApproval){
                // 2.1. If needs to wait make fuzyValue back to future. It will be changed in updateFeature from Master
                if(this.fuzzyValue==null || this.fuzzyValue.isDone()){
                    this.fuzzyValue = new CompletableFuture<>();
                }
            }else{
                // 2.2. If no need to wait simply update the fuzzy value to the updated value & send to master
                this.updateFuzzyValue();
            }
            this.element.sendMessageToMaster(context,ReplicableFeature.prepareMessage(this));
        }
    }

    /**
     * replicationState and part are dependant on graph element attached, need to update them sometimes
     */
    public void syncWithGraphElement(){
        this.replicationState = this.element.getState();
        this.part = this.element.getPart();
    }

    public static GraphQuery prepareMessage(ReplicableFeature el){
        el.syncWithGraphElement();
        return new GraphQuery(el).changeOperation(GraphQuery.OPERATORS.SYNC);
    }

    /**
     * Handles external Features that are coming in
     * @param value
     * @param context
     */
    @Override
    public void updateValue(Feature<T> value,Context context) {
        if(!(value instanceof ReplicableFeature))return;
        ReplicableFeature<T> tmp = (ReplicableFeature<T>) value;
        // Different sync mechanisms for replicas,masters and nones
        if(tmp.replicationState == ReplicableGraphElement.STATE.MASTER && this.replicationState == ReplicableGraphElement.STATE.REPLICA){
            if(tmp.lastSync > this.lastSync){
                // Can be overriten, lastSync check need to prevent delayed messages overriting
                this.lastSync = tmp.lastSync;
                this.replaceValue(tmp.value);
                this.updateFuzzyValue();
            }
        }
        else if(tmp.replicationState== ReplicableGraphElement.STATE.REPLICA && this.replicationState== ReplicableGraphElement.STATE.MASTER){
            if(tmp.lastSync < 0){
                // Incoming is a new replica, needs special attention
                boolean changed = this.handleNewReplica(tmp.value);
                this.updateFuzzyValue();
                if(changed){
                    this.lastSync++;
                    this.element.sendMessageToReplicas(context,ReplicableFeature.prepareMessage(this));
                }else{
                    context.send(Identifiers.PART_TYPE,tmp.part.toString(),ReplicableFeature.prepareMessage(this));
                }
            }else{
                // Replica is not new
                if(tmp.lastSync.equals(this.lastSync)){
                    // Has last version of master node can accept his changes
                   this.lastSync++;
                   this.replaceValue(tmp.value);
                   this.updateFuzzyValue();
                   this.element.sendMessageToReplicas(context,ReplicableFeature.prepareMessage(this));
                }else{
                    // @todo If messages are delivered no need to implement this, but if they can be lost need to send
                }
            }
        }
        else if(tmp.replicationState== ReplicableGraphElement.STATE.NONE && this.replicationState== ReplicableGraphElement.STATE.MASTER){
            // External update need to be commited anyway
            this.lastSync++;
            this.replaceValue(tmp.value);
            this.updateFuzzyValue();
            this.element.sendMessageToReplicas(context,ReplicableFeature.prepareMessage(this));

        }
        else if(tmp.replicationState== ReplicableGraphElement.STATE.NONE && this.replicationState== ReplicableGraphElement.STATE.REPLICA){
            // Redirect to Master node
            this.element.sendMessageToMaster(context,tmp);
        }
    }

    @Override
    public CompletableFuture<T> getValue(){
      return this.fuzzyValue;
    }
}
