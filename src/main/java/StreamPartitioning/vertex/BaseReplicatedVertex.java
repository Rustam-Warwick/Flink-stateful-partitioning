package StreamPartitioning.vertex;
import StreamPartitioning.edges.Edge;
import StreamPartitioning.features.ReplicableFeature;
import StreamPartitioning.types.*;
import StreamPartitioning.features.Feature;
import StreamPartitioning.features.ReplicableArrayListFeature;
import org.apache.flink.statefun.sdk.Context;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * @// TODO: 04/11/2021 Add state resolution in copy()
 */
abstract public class BaseReplicatedVertex extends ReplicableGraphElement implements BaseVertex {
    // 1. Data
    public ReplicableArrayListFeature<Short> inParts = new ReplicableArrayListFeature<>("inParts",this);
    public ReplicableArrayListFeature<Short> outParts = new ReplicableArrayListFeature<>("outParts",this);
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
                Short callerId = Short.valueOf(c.caller().id());

                Stream.concat(inParts.getValue().get().stream(),outParts.getValue().get().stream())
                        .distinct()
                        .filter(item->(item!=null && !item.equals(this.masterPart) && !item.equals(callerId)))
                        .forEach(item->{
                            c.send(Identifiers.PART_TYPE, item.toString(),msg);
                        });
                c.reply(msg);
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
        ReplicableArrayListFeature<Short> updateThis = null;
        if(e.source.equals(this))updateThis=this.outParts;
        else if(e.destination.equals(this))updateThis=this.inParts;
        if(updateThis!=null){
            updateThis.add(this.getPart(),c,true);
        }
    }
    @Override
    public void addVertexCallback(Context c) {
        // 1. Update the current part, of this vertex to the part of this context
        this.setPart(Short.valueOf(c.self().id()));this.inParts.startTimer();
        // 2. Sync Features which are remote with the parent.
        try{
            ArrayList<Field> fields = BaseReplicatedVertex.getReplicableFeatures(this);
            for(Field f:fields){
                ReplicableFeature<?> tmp = (ReplicableFeature) f.get(this);
                tmp.sync(c,true);
            }
        }
        catch (IllegalAccessException e){
            System.out.println(e);
        }

    }
    @Override
    public void updateFeatureCallback(Context c,Feature f){
        // New update request for a feature
       try{
           Field a = BaseReplicatedVertex.getReplicatedFeature(this,f.fieldName);
           Feature value = (Feature) a.get(this);
           value.updateValue(f,c);
       }catch (Exception e){
           System.out.println(e);
       }
    }
    // Abstracts
    abstract public BaseReplicatedVertex copy();
    abstract public CompletableFuture<? extends Object> getFeature(short l);
    // Static Methods
    public static ArrayList<Field> getReplicableFeatures(BaseReplicatedVertex el){
        Class<?> tmp = null;
        ArrayList<Field> fields = new ArrayList<>();
        do{
            if(tmp==null) tmp=el.getClass();
            else tmp = tmp.getSuperclass();
            Field[] fieldsForClass = tmp.getDeclaredFields();
            for(Field tmpField:fieldsForClass){
                if(ReplicableFeature.class.isAssignableFrom(tmpField.getType()))fields.add(tmpField);
            }
        }
        while(!tmp.equals(BaseReplicatedVertex.class));
        return fields;
    }
    public static Field getReplicatedFeature(BaseReplicatedVertex el,String fieldName) throws NoSuchFieldException{
        Class <?> tmp = null;
        Field res = null;
        do{
            if(tmp==null) tmp=el.getClass();
            else tmp = tmp.getSuperclass();
            try{
                Field tmpField = tmp.getDeclaredField(fieldName);
                res = tmpField;
                break;
            }catch (Exception e){
                // no need to do anything
            }
        }
        while(!tmp.equals(BaseReplicatedVertex.class));
        if(res==null) throw new NoSuchFieldException("Field not found") ;
        return res;
    }
}
