package StreamPartitioning.types;

import org.apache.flink.statefun.sdk.Context;

abstract public class ReplicableGraphElement extends GraphElement {
    public Short masterPart = -1;
    public enum STATE {NONE,REPLICA,MASTER}

    public ReplicableGraphElement(){
        super();this.masterPart=-1;
    }
    public ReplicableGraphElement(String id){
        super(id);this.masterPart=-1;
    }
    public ReplicableGraphElement(String id,Short part){
        super(id,part);
        this.masterPart=-1;
    }
    public ReplicableGraphElement(String id,Short part,Short masterPart){
        super(id,part);
        this.masterPart = masterPart;
    }

    public abstract void sendMessageToReplicas(Context c, Object msg);
    public void sendMessageToMaster(Context c, Object msg){
        if(this.getState()==STATE.REPLICA){
            c.send(Identifiers.PART_TYPE, this.masterPart.toString(), msg);
        }
    }

    public STATE getState(){
        if(this.masterPart==null)return STATE.MASTER;
        else if(this.masterPart==-1)return STATE.NONE;
        return STATE.REPLICA;
    }
    public Short getMasterPart() {
        return masterPart;
    }

    public void setMasterPart(Short masterPart) {
        this.masterPart = masterPart;
    }
}
