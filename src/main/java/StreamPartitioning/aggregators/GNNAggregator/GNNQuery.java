package StreamPartitioning.aggregators.GNNAggregator;

import StreamPartitioning.vertex.Vertex;

public class GNNQuery {
    public enum OPERATORS {NONE, AGG_REQ,AGG_RES,UPD_REQ}

    public String vertexId = null;
    public short l = 1;
    public OPERATORS op = OPERATORS.NONE;
    public Object agg = null;
    public String uuid = null;

    public GNNQuery withId(String id){
        uuid = id;
        return this;
    }

    public GNNQuery withAggValue(Object a){
        this.agg = a;
        return this;
    }
    public GNNQuery withOperator(OPERATORS op){
            this.op = op;
            return this;
    }

    public GNNQuery withLValue(short L){
        this.l = L;
        return this;
    }

    public GNNQuery withVertex(String v){
        this.vertexId = v;
        return this;
    }



}
