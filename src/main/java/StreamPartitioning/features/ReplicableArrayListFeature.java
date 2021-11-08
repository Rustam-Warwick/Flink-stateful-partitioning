package StreamPartitioning.features;
import StreamPartitioning.types.ReplicableGraphElement;
import org.apache.flink.statefun.sdk.Context;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Predicate;

public class ReplicableArrayListFeature<TP> extends ReplicableFeature<ArrayList<TP>> {
    public ReplicableArrayListFeature(){
        super();
    }
    public ReplicableArrayListFeature(String fieldName, ReplicableGraphElement el){
        super(fieldName,new ArrayList<>(),el);
    }

    @Override
    public boolean handleNewReplica( ArrayList<TP> elem) {
        // Merge the incoming replica values with the current value
        boolean changed = false;
        for (TP aShort : elem) {
            if (!this.value.contains(aShort)) {
                changed = true;
                this.value.add(aShort);
            }
        }
        return changed;
    }

    @Override
    public void replaceValue(ArrayList<TP> elem) {
        this.value.clear();
        this.value.addAll(elem);
    }

    public void startTimer(){
        if(this.attachedId==null ||  !this.attachedId.equals("3"))return;
        Timer a = new Timer();
        ReplicableArrayListFeature<TP> as = this;
        a.schedule(new TimerTask() {
            @Override
            public void run() {
                StringBuilder values = new StringBuilder();
                for(TP a: as.value) values.append(a.toString()+" ");
                System.out.format("Part %s  Size:%s  Values:%s ReplicationState:%s Updated last:%s \n",as.part,as.value.size(),values,as.replicationState,as.lastSync);
            }
        },0,10000);
    }

    /**
     * Add element to array, thread safe and synchronized acorss the replicas
     * @param el
     * @param c
     * @param forceAddition
     */
    public void add( TP el, Context c,boolean forceAddition){
        Predicate<ArrayList<TP>> pr = null;
        if(forceAddition) pr = tps -> !tps.contains(el);
        this.editValue(list->{
           list.add(el);
        },c,pr);
    }




}

