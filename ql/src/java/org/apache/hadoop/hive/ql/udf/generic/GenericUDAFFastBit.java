package org.apache.hadoop.hive.ql.udf.generic;

import javaewah.EWAHCompressedBitmap;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by malmee on 12/25/16.
 */
public class GenericUDAFFastBit extends AbstractGenericUDAFResolver {
    static final Logger LOG = LoggerFactory.getLogger(GenericUDAFFastBit.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        LOG.info("parameters in Genericfastbit: "+ parameters[0]);
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        LOG.info("oi in Genericfastbit: "+ oi);
        if (!ObjectInspectorUtils.compareSupported(oi)) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Cannot support comparison of map<> type or complex type containing map<>.");
        }
        return new GenericUDAFFastBitEvaluator();
    }


    public static class GenericUDAFFastBitEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector inputOI;

        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations
        // (lists of bitmaps)
        private transient StandardListObjectInspector loi;
        private transient StandardListObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {

            super.init(m, parameters);
            // init output object inspectors
            // The output of a partial aggregation is a list
            if (m == Mode.PARTIAL1) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory
                        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
            } else if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
                internalMergeOI = (StandardListObjectInspector) parameters[0];
                inputOI = (PrimitiveObjectInspector)internalMergeOI.getListElementObjectInspector();
                loi = (StandardListObjectInspector) ObjectInspectorFactory
                        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                return loi;
            } else { // Mode.COMPLETE, ie. no map-side aggregation, requires ordering
                inputOI = (PrimitiveObjectInspector)parameters[0];
                loi = (StandardListObjectInspector) ObjectInspectorFactory
                        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                return loi;
            }
        }

        /** class for storing the current partial result aggregation */
        @AggregationType(estimable = true)
        static class BitmapAgg extends AbstractAggregationBuffer {
            EWAHCompressedBitmap bitmap;
            @Override
            public int estimate() {
                return bitmap.sizeInBytes();
            }
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {

            ((GenericUDAFFastBit.GenericUDAFFastBitEvaluator.BitmapAgg) agg).bitmap = new EWAHCompressedBitmap();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            GenericUDAFFastBit.GenericUDAFFastBitEvaluator.BitmapAgg result = new GenericUDAFFastBit.GenericUDAFFastBitEvaluator.BitmapAgg();
            reset(result);
            return result;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {

        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return null;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {

        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            return null;
        }
    }
}

