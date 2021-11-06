package StreamPartitioning.vertex;
import StreamPartitioning.edges.Edge;
import StreamPartitioning.types.*;
import StreamPartitioning.features.Feature;
import StreamPartitioning.features.RemoteShortArrayListFeature;
import org.apache.flink.statefun.sdk.Context;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * @// TODO: 04/11/2021 Add state resolution in copy()
 */
public class BaseReplicatedVertex extends ReplicableGraphElement implements BaseVertex {
    // 1. Data
    public RemoteShortArrayListFeature inParts = new RemoteShortArrayListFeature("inParts",this);
    public RemoteShortArrayListFeature outParts = new RemoteShortArrayListFeature("outParts",this);
    // 2. Constructors
    public BaseReplicatedVertex(){
        super();
    }
    public BaseReplicatedVertex(String id){
        super(id);
    }
    public BaseReplicatedVertex(String id, Short part){
        super(id,part);
    }
    public BaseReplicatedVertex(String id, Short part,Short masterPart){
        super(id,part,masterPart);
    }
    // 3. Communication helpers
    @Override
    public void sendMessageToReplicas(Context c,Object msg){
        CompletableFuture.allOf(inParts.getValue(),outParts.getValue()).whenComplete((vzoid,err)->{
            try{
                Stream.concat(inParts.getValue().get().stream(),outParts.getValue().get().stream())
                        .distinct()
                        .forEach(item->{
                            if(item==null)return;
                            c.send(Identifiers.PART_TYPE, item.toString(),msg);
                        });
            }catch(Exception e){

            }
        });

    }
    public void sendThisToOutPartsAsync(Context c, Object msg){
        this.outParts.getValue().whenComplete((list,exception)->{
            list.forEach(item->{
                c.send(Identifiers.PART_TYPE,item.toString(),msg);
            });
        });
    }
    public void sendThisToInPartsAsync(Context c, Object msg){
        this.inParts.getValue().whenComplete((list,exception)->{
            list.forEach(item->{
                c.send(Identifiers.PART_TYPE,item.toString(),msg);
            });
        });
    }

    // 4. Callbacks
    @Override
    public void addEdgeCallback(Edge e, Context c){
        RemoteShortArrayListFeature updateThis = null;
        if(e.source.equals(this))updateThis=this.outParts;
        else if(e.destination.equals(this))updateThis=this.inParts;
        if(updateThis!=null){
            updateThis.add(this.getPart(),c);
        }
    }
    @Override
    public void addVertexCallback(Context c){
        this.setPart(Short.valueOf(c.self().id()));this.inParts.startTimer();
    }
    @Override
    public void updateFeatureCallback(Context c,Feature f){
       try{
           Field a = this.getClass().getDeclaredField(f.fieldName);
           Feature value = (Feature) a.get(this);
           value.updateValue(f,c);
       }catch (Exception e){

       }
    }
    // Overrides
    public BaseReplicatedVertex copy() {
        BaseReplicatedVertex tmp = new BaseReplicatedVertex(this.id,this.part,this.masterPart);
        return tmp;
    }

}
