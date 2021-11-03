package StreamPartitioning.vertex;

import StreamPartitioning.types.GraphElement;
import StreamPartitioning.types.Identifiers;
import StreamPartitioning.types.UserQuery;
import org.apache.flink.statefun.sdk.Context;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Stream;

abstract public class BaseReplicatedVertex implements GraphElement {
    public String id = null; // Id of the vertex, must be unique
    public Short part = null; // Part where this vertex is located at
    public Short masterPart = null; // Id of the master Vertex Part, used if vertex-cut partitioning
    public String lastSync = null; // Last sync with the master
    public ArrayList<Short> inParts = new ArrayList<>(); // parts with in-Neighbors
    public ArrayList<Short> outParts = new ArrayList<>(); // parts with out-Neighbors
    public transient CompletableFuture<ArrayList<Short>> fuzzyInParts = new CompletableFuture<>();
    public transient CompletableFuture<ArrayList<Short>> fuzzyOutParts = new CompletableFuture<>();
    // Abstract Functions
    public abstract void mergeWithIncoming(BaseReplicatedVertex incoming);

    // Main Logic Functions
    public void sendThisToReplicas(Context c,Object msg){
        Stream.concat(this.inParts.stream(),this.outParts.stream())
                .distinct()
                .filter(item->item!=this.part)
                .forEach(item->{
                    System.out.println(item);
                    c.send(Identifiers.PART_TYPE,item.toString(),msg);
                });
    }

    public void updateMaster(Context c){
        if(this.isPlaceholder()){
            UserQuery query = new UserQuery(this).changeOperation(UserQuery.OPERATORS.UPDATE);
            c.send(Identifiers.PART_TYPE, this.getMasterId().toString(), query);
        }
    }
    public void sync(Context c,BaseReplicatedVertex incoming) {
        // 1. Merge incoming message with this copy
        this.mergeWithIncoming(incoming);
        this.fuzzyInParts.complete(this.inParts);
        this.fuzzyOutParts.complete(this.outParts);
        if(this.isMaster()){
            // 1. Send updated master node to all replicas
            this.lastSync = new Date().toString();
            UserQuery query = new UserQuery(this).changeOperation(UserQuery.OPERATORS.UPDATE);
            this.sendThisToReplicas(c,query);
        }
        else{
            System.out.println("Non Master Sync");
        }
    }


    // getters, setters & helpers
    public Short getMasterId() {
        return masterPart;
    }
    public BaseReplicatedVertex withId(String id){
        this.id = id;
        return this;
    }
    public BaseReplicatedVertex withMasterId(Short id){
        this.masterPart = id;
        return this;
    }
    public boolean isMaster(){
        return masterPart==null;
    }
    public boolean isPlaceholder(){
        return !this.isMaster() && this.lastSync==null;
    }
    public CompletableFuture<ArrayList<Short>> getInParts(){
        return fuzzyInParts;
    }
    public CompletableFuture<ArrayList<Short>> getOutParts(){
        return fuzzyOutParts;
    }



//    OVERRIDES
    @Override
    public Short getPart() {
        return part;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean equals(GraphElement e) {
        return getId()==e.getId();
    }
}
