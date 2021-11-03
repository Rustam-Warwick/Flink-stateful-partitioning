package StreamPartitioning.types;



public class UserQuery {
    public enum OPERATORS {NONE, ADD, REMOVE,UPDATE}

    public GraphElement element; // Element over which we are querying
    public OPERATORS op = OPERATORS.NONE;
    public UserQuery(){
        this.element = null;
    }

    public UserQuery(GraphElement element) {
        this.element = element;
    }

    public UserQuery changeOperation(OPERATORS op){
        this.op = op;
        return this;
    }
}
