package StreamPartitioning.vertex;
import StreamPartitioning.types.*;
import StreamPartitioning.features.Feature;
import StreamPartitioning.features.RemoteArrayListFeature;
import org.apache.flink.statefun.sdk.Context;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * @// TODO: 04/11/2021 Add state resolution in copy()
 */
public class BaseReplicatedVertex extends BaseVertex {
    // 1. Data
    public Short masterPart = null;
    public RemoteArrayListFeature inParts = new RemoteArrayListFeature(this,"inParts");
    public RemoteArrayListFeature outParts = new RemoteArrayListFeature(this,"outParts");
    // 2. Constructors
    public BaseReplicatedVertex(Short masterPart,Short part,String id) {
        super(id, part);
        this.masterPart = masterPart;
    }
    public BaseReplicatedVertex(Short masterPart,String id) {
        super(id);
        this.masterPart = masterPart;
    }
    public BaseReplicatedVertex(String id){
        super(id);
        this.masterPart = null;
    }
    // 3. Communication helpers
    public void sendThisToReplicas(Context c,Object msg){
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
    public void sendThisToMaster(Context c,Object o){
        c.send(Identifiers.PART_TYPE, this.masterPart.toString(), o);
    }

    // 4. Callbacks
    @Override
    public void addEdgeCallback(Edge e,Context c){
        RemoteArrayListFeature updateThis = null;
        if(e.source.equals(this))updateThis=this.outParts;
        else if(e.destination.equals(this))updateThis=this.inParts;
        if(updateThis!=null){
            updateThis.add(this.getPart(),c);
        }
    }
    @Override
    public void addVertexCallback(Context c){
        this.setPart(Short.valueOf(c.self().id()));
    }
    @Override
    public void updateVertexCallback(Context c,Feature f){
       try{
           Field a = this.getClass().getDeclaredField(f.fieldName);
           Feature value = (Feature) a.get(this);
           value.setValue(f,c);
       }catch (Exception e){

       }
    }
    // Overrides
    public BaseReplicatedVertex copy() {
        BaseReplicatedVertex tmp = new BaseReplicatedVertex(this.masterPart,this.part,this.id);
        return tmp;
    }

     // 4. Helpers Setters
    public void setMasterPart(Short e){
        this.masterPart = e;
     }
    public Short getMasterPart(){
        return this.masterPart;
    }

    public boolean isMaster(){
        return masterPart==null;
    }

}
