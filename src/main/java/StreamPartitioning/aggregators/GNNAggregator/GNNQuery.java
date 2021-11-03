package StreamPartitioning.aggregators.GNNAggregator;

import StreamPartitioning.vertex.Vertex;

public class GNNQuery {
    public enum OPERATORS {NONE, AGG_REQ,AGG_RES,UPD_REQ}
    public Vertex vertex = null;
    public short L = 1;
    public OPERATORS op = OPERATORS.NONE;
    public Integer agg = 0;
    public String uuid = null;
    public GNNQuery withId(String id){
        uuid = id;
        return this;
    }
    public GNNQuery withOperator(OPERATORS op){
            this.op = op;
            return this;
    }
    public GNNQuery withLValue(short L){
        this.L = L;
        return this;
    }
    public GNNQuery withVertex(Vertex v){
        this.vertex = v;
        return this;
    }



}
