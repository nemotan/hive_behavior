package com.landray.hive.ql;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.io.Serializable;
import java.util.*;

/**
 * Created by nemo on 15/9/24.
 */
public class GenericUDAFMkCollectionEvaluatorSize extends GenericUDAFEvaluator
        implements Serializable {
    private static final long serialVersionUID = 1l;

    enum BufferType {SET, LIST}

    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private transient PrimitiveObjectInspector inputOI;
    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
    // of objs)
    private transient StandardListObjectInspector loi;

    private transient ListObjectInspector internalMergeOI;

    private BufferType bufferType;

    //needed by kyro
    public GenericUDAFMkCollectionEvaluatorSize() {
    }

    public GenericUDAFMkCollectionEvaluatorSize(BufferType bufferType) {
        this.bufferType = bufferType;
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
            throws HiveException {
        super.init(m, parameters);
        // init output object inspectors
        // The output of a partial aggregation is a list
        if (m == Mode.PARTIAL1) {
            inputOI = (PrimitiveObjectInspector) parameters[0];
            return ObjectInspectorFactory
                    .getStandardListObjectInspector((PrimitiveObjectInspector) ObjectInspectorUtils
                            .getStandardObjectInspector(inputOI));
        } else {
            if (!(parameters[0] instanceof ListObjectInspector)) {
                //no map aggregation.
                inputOI = (PrimitiveObjectInspector) ObjectInspectorUtils
                        .getStandardObjectInspector(parameters[0]);
                return (StandardListObjectInspector) ObjectInspectorFactory
                        .getStandardListObjectInspector(inputOI);
            } else {
                internalMergeOI = (ListObjectInspector) parameters[0];
                inputOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
                loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                return loi;
            }
        }
    }


    class MkArrayAggregationBuffer extends AbstractAggregationBuffer {

        private Collection<Object> container;

        public MkArrayAggregationBuffer() {
            if (bufferType == BufferType.LIST) {
                container = new ArrayList<Object>();
            } else if (bufferType == BufferType.SET) {
                container = new LinkedHashSet<Object>();
            } else {
                throw new RuntimeException("Buffer type unknown");
            }
        }
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
        ((MkArrayAggregationBuffer) agg).container.clear();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
        return ret;
    }

    //mapside
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
            throws HiveException {
        assert (parameters.length == 1);
        Object p = parameters[0];

        if (p != null) {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            putIntoCollection(p, myagg);
        }
    }

    //mapside
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
        MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
        List<Object> ret = new ArrayList<Object>(myagg.container.size());
        ret.addAll(myagg.container);

        Collections.sort(ret, new Comparator() {
            public int compare(Object o1, Object o2) {
                long v1 = Long.valueOf(o1.toString());
                long v2 = Long.valueOf(o2.toString());
                return v2 > v1 ? -1 : 1;
            }
        });
        if(ret.size()>20){
            ret.subList(0, ret.size()-20).clear();
        }
        return ret;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
            throws HiveException {
        MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
        List<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
        if (partialResult != null) {
            for (Object i : partialResult) {
                putIntoCollection(i, myagg);
            }
        }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
        MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
        List<Object> ret = new ArrayList<Object>(myagg.container.size());
        ret.addAll(myagg.container);
        return ret;
    }

    private void putIntoCollection(Object p, MkArrayAggregationBuffer myagg) {
        Object pCopy = ObjectInspectorUtils.copyToStandardObject(p, this.inputOI);
        myagg.container.add(pCopy);
    }

    public BufferType getBufferType() {
        return bufferType;
    }

    public void setBufferType(BufferType bufferType) {
        this.bufferType = bufferType;
    }
}
