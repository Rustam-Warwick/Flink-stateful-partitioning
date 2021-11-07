package StreamPartitioning.features;

import StreamPartitioning.types.GraphElement;
import org.apache.flink.statefun.sdk.Context;

import java.util.concurrent.CompletableFuture;

abstract public class Feature<T> {
    abstract public CompletableFuture<T> getValue();
    abstract public void updateValue(Feature<T> v, Context context);

    public String fieldName;
    public String attachedId=null;
    public String attachedToClassName=null;

    public Feature(){
        this.fieldName = null;
        this.attachedId = null;
        this.attachedToClassName = null;
    }
    public Feature(String fieldName) {
        this.fieldName = fieldName;
        this.attachedId = null;
        this.attachedToClassName = null;
    }
    public Feature(String fieldName,GraphElement element){
        this.fieldName = fieldName;
        this.attachedId = element.getId();
        this.attachedToClassName = element.getClass().getName();
    }

   @Override
    public boolean equals(Object e){
        Feature<?> s = (Feature) e;
        return s.fieldName.equals(this.fieldName);
   }
}
