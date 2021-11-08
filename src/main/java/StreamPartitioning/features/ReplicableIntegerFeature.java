package StreamPartitioning.features;

import StreamPartitioning.types.ReplicableGraphElement;

import java.util.concurrent.atomic.AtomicInteger;

public class ReplicableIntegerFeature extends ReplicableFeature<Integer[]>{
    public ReplicableIntegerFeature(){
        super();
    }

    @Override
    public void replaceValue(Integer[] elem) {
        this.value[0] = elem[0];
    }

    @Override
    public boolean handleNewReplica(Integer[] elem) {
        return false;
    }

    public ReplicableIntegerFeature(String fieldName, ReplicableGraphElement el){
        super(fieldName,new Integer[]{0},el);
    }


}
