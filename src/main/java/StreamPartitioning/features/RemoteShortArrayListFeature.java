package StreamPartitioning.features;
import StreamPartitioning.types.ReplicableGraphElement;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.statefun.sdk.Context;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

public class RemoteShortArrayListFeature extends ReplicableFeature<ArrayList<Short>> {
    public RemoteShortArrayListFeature(){
        super();
    }
    public RemoteShortArrayListFeature(String fieldName, ReplicableGraphElement el){
        super(fieldName,new ArrayList<>(),el);
    }

    @Override
    public boolean handleNewReplica(ArrayList<Short> elem) {
        boolean changed = false;
        for(int i=0;i<elem.size();i++){
            if(!this.value.contains(elem.get(i))){
                this.value.add(elem.get(i));
                changed = true;
            }
        }
        return changed;
    }

    public void startTimer(){
        if(this.attachedId==null || !this.attachedId.equals("3"))return;
        if(this.element.part==null){
            System.out.println("");
        }
        Timer a = new Timer();
        RemoteShortArrayListFeature as = this;
        a.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.format("Part %s  Size:%s  ReplicationState:%s \n",as.part,as.value.size(),as.replicationState);
            }
        },0,2200);
    }

    public void add( Short el, Context c){
        this.getValue().whenComplete((list,tr)->{
            if(!list.contains(el)){
                list.add(el);
                this.sync(c,true);
            }
        });
    }




}

