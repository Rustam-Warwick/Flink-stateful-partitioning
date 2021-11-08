package StreamPartitioning.features;

import StreamPartitioning.types.GraphQuery;
import StreamPartitioning.types.Identifiers;
import StreamPartitioning.types.ReplicableGraphElement;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.statefun.sdk.Context;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;

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
        this.fuzzyValue.complete(this.value);
        this.part = null;
    }
    public ReplicableFeature(String fieldName, T value, ReplicableGraphElement el){
        super(fieldName,el);
        this.value = value;
        this.element = el;
        this.lastSync = -1;
        this.replicationState = el.getState();
        this.fuzzyValue = new CompletableFuture<>();
        this.fuzzyValue.complete(this.value);
        this.part = el.getPart();
    }
    public abstract boolean handleNewReplica(T elem);
    abstract public void replaceValue(T elem);

    /**
     * Wrapper for all edits. Should be used insed of directly modifying the value
     * @param fn
     * @param c
     * @param forceEditIf
     */
    public void editValue(Consumer<T> fn, Context c, @Nullable Predicate<T> forceEditIf){

        this.getValue()
                .orTimeout(5,TimeUnit.SECONDS).whenComplete((list,a)->{
                    if(a instanceof TimeoutException){
                        this.editValue(fn,c,forceEditIf);
                        System.out.println("TIMEOUT");
                    }
                    synchronized (list){
                        if(forceEditIf!=null && !forceEditIf.test(list))return;
                        fn.accept(list);
                        this.sync(c,true);
                        if(forceEditIf!=null)this.editValue(fn,c,forceEditIf);
                    }
                });


    }
    /**
     * New Replica has joined. Merge the values
     * @param elem
     * @return
     */
    public boolean handleNewReplicaMessage(T elem){
        boolean isChanged = false;
        synchronized (this.value){
            isChanged = this.handleNewReplica(elem);
            if(isChanged)this.lastSync++;
        }
        return isChanged;
    }

    /**
     * Old replica is sending a new update
     * @param elem
     * @return
     */
    public boolean handleReplicaMessage(ReplicableFeature<T> elem){
        if(this.attachedId.equals("3")){
            System.out.println("here");
        }
        if(elem.lastSync.equals(this.lastSync)){
            // Has last version of master node can accept his changes
            synchronized (this.value){
                this.lastSync++;
                this.replaceValue(elem.value);
            }
            return true;
        }else{
            // @todo If messages are delivered no need to implement this, but if they can be lost need to send
        }
        return true;
    }

    /**
     * Master is sending message to a replica
     * @param elem
     * @return
     */
    public void handleMasterMessage(ReplicableFeature<T> elem){
        if(elem.lastSync >= this.lastSync){
            // This was the last message commited well
            synchronized (this.value){
                this.lastSync = elem.lastSync;
                this.replaceValue(elem.value);
                this.completeFuzzyValue();
            }
        }
        else{

        }
    }

    /**
     * Make Future value
     */
    public void waitForFuzzyValue(){
        if(this.fuzzyValue!=null && !this.fuzzyValue.isDone())return;
        this.fuzzyValue = new CompletableFuture<>();
    }

    /**
     * Comple the future value
     */
    public void completeFuzzyValue(){
        if(this.fuzzyValue==null){
            this.fuzzyValue = new CompletableFuture<>();
            this.fuzzyValue.complete(this.value);
        }
        else if(!this.fuzzyValue.isDone())this.fuzzyValue.complete(this.value);
    }

    /**
     * Handles direct changes of the value. Direct changes of replicas have to be waited until master approves!
     * Be very careful with this, do not call if the state is not changed
     * @param context
     */
    public void sync(Context context,boolean waitForMasterApproval){
        // Value explicitly changed need to sync
        this.syncWithGraphElement();
        if(this.replicationState== ReplicableGraphElement.STATE.MASTER){
            // 1. If master simply notify the replicas about this value
            this.lastSync++;
            this.element.sendMessageToReplicas(context,ReplicableFeature.prepareMessage(this));
        }
        else if(this.replicationState==ReplicableGraphElement.STATE.REPLICA){
            // 2. If replica
            if(waitForMasterApproval){
                // 2.1. If needs to wait make fuzyValue back to future. It will be changed in updateFeature from Master
                this.waitForFuzzyValue();
            }else{
                // 2.2. If no need to wait simply update the fuzzy value to the updated value & send to master
                this.completeFuzzyValue();
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

    /**
     * Prepare the Feature Query for sending
     * @param el
     * @return
     */
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
                this.handleMasterMessage(tmp);
        }
        else if(tmp.replicationState== ReplicableGraphElement.STATE.REPLICA && this.replicationState== ReplicableGraphElement.STATE.MASTER){
            if(tmp.lastSync < 0){
                // Incoming is a new replica, needs special attention
                if(this.handleNewReplicaMessage(tmp.value))this.element.sendMessageToReplicas(context,ReplicableFeature.prepareMessage(this));
                else context.send(Identifiers.PART_TYPE,tmp.part.toString(),ReplicableFeature.prepareMessage(this));

            }else{
                // Replica is not new
               if(this.handleReplicaMessage(tmp))this.element.sendMessageToReplicas(context,ReplicableFeature.prepareMessage(this));
            }
        }
        else if(tmp.replicationState== ReplicableGraphElement.STATE.NONE && this.replicationState== ReplicableGraphElement.STATE.MASTER){
            // External update need to be commited anyway
            this.lastSync++;
            this.replaceValue(tmp.value);
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
