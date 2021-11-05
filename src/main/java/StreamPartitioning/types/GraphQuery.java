package StreamPartitioning.types;


public class GraphQuery {
    public enum OPERATORS {NONE, ADD, REMOVE,UPDATE,SYNC}

    public Object element=null; // Element over which we are querying
    public OPERATORS op = OPERATORS.NONE;
    public GraphQuery(){
        this.element = null;
    }

    public GraphQuery(Object element) {
        this.element = element;
    }
    public GraphQuery changeOperation(OPERATORS op){
        this.op = op;
        return this;
    }
}
