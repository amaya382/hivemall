package hivemall.tools.matrix;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;
import hivemall.utils.lang.Preconditions;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Description(name = "transpose_and_dot",
        value = "_FUNC_(array<number> matrix0_row|single_vector0, array<double> matrix1_row|single_vector1)" +
                " - Returns dot(matrix0.T, matrix1) as array<array<double>>, shape = (matrix0.#cols, matrix1.#cols)")
public class TransposeAndDotUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        ObjectInspector[] OIs = info.getParameterObjectInspectors();

        if (OIs.length != 2) {
            throw new UDFArgumentLengthException("Specify 2 arguments.");
        }

        if (!HiveUtils.isNumberListOI(OIs[0])) {
            throw new UDFArgumentTypeException(0, "Only array<double> type argument is acceptable but "
                    + OIs[0].getTypeName() + " was passed as `matrix0_row`");
        }

        if (!HiveUtils.isNumberListOI(OIs[1])) {
            throw new UDFArgumentTypeException(1, "Only array<double> type argument is acceptable but "
                    + OIs[1].getTypeName() + " was passed as `matrix1_row`");
        }

        return new TransposeAndDotUDAFEvaluator();
    }

    private static class TransposeAndDotUDAFEvaluator extends GenericUDAFEvaluator {
        // PARTIAL1 and COMPLETE
        private ListObjectInspector matrix0RowOI;
        private PrimitiveObjectInspector matrix0ElOI;
        private ListObjectInspector matrix1RowOI;
        private PrimitiveObjectInspector matrix1ElOI;

        // PARTIAL2 and FINAL
        private ListObjectInspector aggMatrixOI;
        private ListObjectInspector aggMatrixRowOI;
        private DoubleObjectInspector aggMatrixElOI;

        @AggregationType(estimable = true)
        static class TransposeAndDotAggregationBuffer extends AbstractAggregationBuffer {
            double[][] aggMatrix;

            @Override
            public int estimate() {
                return aggMatrix != null
                        ? aggMatrix.length * aggMatrix[0].length * 8
                        : 0;
            }

            public void init(int n, int m) {
                aggMatrix = new double[n][m];
            }

            public void reset() {
                if (aggMatrix != null) {
                    for (double[] row : aggMatrix) {
                        Arrays.fill(row, 0.0);
                    }
                }
            }
        }

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] OIs) throws HiveException {
            super.init(mode, OIs);

            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                matrix0RowOI = (ListObjectInspector) OIs[0];
                matrix0ElOI = HiveUtils.asDoubleCompatibleOI(matrix0RowOI.getListElementObjectInspector());
                matrix1RowOI = (ListObjectInspector) OIs[1];
                matrix1ElOI = HiveUtils.asDoubleCompatibleOI(matrix1RowOI.getListElementObjectInspector());
            } else {
                aggMatrixOI = (ListObjectInspector) OIs[0];
                aggMatrixRowOI = (ListObjectInspector) aggMatrixOI.getListElementObjectInspector();
                aggMatrixElOI = (DoubleObjectInspector) aggMatrixRowOI.getListElementObjectInspector();
            }

            return ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
        }

        @Override
        public AbstractAggregationBuffer getNewAggregationBuffer() throws HiveException {
            TransposeAndDotAggregationBuffer myAgg = new TransposeAndDotAggregationBuffer();
            reset(myAgg);
            return myAgg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            TransposeAndDotAggregationBuffer myAgg = (TransposeAndDotAggregationBuffer) agg;
            myAgg.reset();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            TransposeAndDotAggregationBuffer myAgg = (TransposeAndDotAggregationBuffer) agg;

            double[] matrix0Row = HiveUtils.asDoubleArray(parameters[0], matrix0RowOI, matrix0ElOI);
            double[] matrix1Row = HiveUtils.asDoubleArray(parameters[1], matrix1RowOI, matrix1ElOI);

            Preconditions.checkNotNull(matrix0Row);
            Preconditions.checkNotNull(matrix1Row);

            if (myAgg.aggMatrix == null) {
                myAgg.init(matrix0Row.length, matrix1Row.length);
            }

            for (int i = 0; i < matrix0Row.length; i++) {
                for (int j = 0; j < matrix1Row.length; j++) {
                    myAgg.aggMatrix[i][j] += matrix0Row[i] * matrix1Row[j];
                }
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object other) throws HiveException {
            if (other == null) {
                return;
            }

            TransposeAndDotAggregationBuffer myAgg = (TransposeAndDotAggregationBuffer) agg;

            List matrix = aggMatrixOI.getList(other);
            for (int i = 0; i < matrix.size(); i++) {
                double[] row = HiveUtils.asDoubleArray(matrix.get(i), aggMatrixRowOI, aggMatrixElOI);

                if (myAgg.aggMatrix == null) {
                    myAgg.init(matrix.size(), row.length);
                }

                for (int j = 0; j < row.length; j++) {
                    myAgg.aggMatrix[i][j] += row[j];
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            TransposeAndDotAggregationBuffer myAgg = (TransposeAndDotAggregationBuffer) agg;

            List<List<DoubleWritable>> result = new ArrayList<List<DoubleWritable>>();
            for (double[] row : myAgg.aggMatrix) {
                result.add(WritableUtils.toWritableList(row));
            }
            return result;
        }
    }
}
