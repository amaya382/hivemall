package hivemall.tools.matrix;

import hivemall.utils.hadoop.HiveUtils;
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
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Description(name = "column_dot_array",
        value = "_FUNC_(number col, const array<number> row) - Returns matrix as array<array<double>>, shape = (#rows, #columns)")
public class ColumnDotArrayUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        ObjectInspector[] OIs = info.getParameterObjectInspectors();

        if (OIs.length != 2) {
            throw new UDFArgumentLengthException("Specify two arguments.");
        }

        if (!HiveUtils.isNumberOI(OIs[0])) {
            throw new UDFArgumentTypeException(0, "Only number type arguments are accepted but "
                    + OIs[0].getTypeName() + " was passed as `col`");
        }

        if (!HiveUtils.isListOI(OIs[1])
                || !HiveUtils.isNumberOI(((ListObjectInspector) OIs[1]).getListElementObjectInspector())) {
            throw new UDFArgumentTypeException(1,
                    "Only array<number> type arguments are accepted but " + OIs[1].getTypeName()
                            + " was passed as `row`");
        }

        return new ColumnDotArrayGenericUDAFEvaluator();
    }

    private static class ColumnDotArrayGenericUDAFEvaluator extends GenericUDAFEvaluator {
        // common
        private ListObjectInspector rowOI;
        private DoubleObjectInspector rowElOI;

        // PARTIAL1 and COMPLETE
        private DoubleObjectInspector colOI;

        // PARTIAL2 and FINAL
        private StructObjectInspector structOI;
        private StructField rowField, matrixField ;
        private ListObjectInspector matrixOI;
        private ListObjectInspector matrixRowOI;
        private DoubleObjectInspector matrixRowElOI;

        @AggregationType(estimable = true)
        static class ColumnDotArrayAggregationBuffer extends AbstractAggregationBuffer {
            double[] row;
            List<double[]> matrix;

            @Override
            public int estimate() {
                return matrix.size()*row.length*8; // estimating value for size of *this* buffer
            }

            public void reset() {
                matrix = new ArrayList<double[]>();
                // TODO: zero fill?
            }
        }

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] OIs) throws HiveException {
            super.init(mode, OIs);

            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                colOI = (DoubleObjectInspector) OIs[0];
                rowOI = (ListObjectInspector) OIs[1];
                rowElOI = (DoubleObjectInspector)rowOI.getListElementObjectInspector();
            } else {
                structOI = (StructObjectInspector)OIs[0];
                rowField=structOI.getStructFieldRef("row");
                matrixField = structOI.getStructFieldRef("matrix");
                rowOI=(ListObjectInspector)rowField.getFieldObjectInspector();
                rowElOI=(DoubleObjectInspector)rowOI.getListElementObjectInspector();
                matrixOI = (ListObjectInspector) matrixField.getFieldObjectInspector();
                matrixRowOI = (ListObjectInspector) matrixOI.getListElementObjectInspector();
                matrixRowElOI = (DoubleObjectInspector) matrixRowOI.getListElementObjectInspector();
            }

            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
                fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
                fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorFactory.getStandardListObjectInspector(
                                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)));
                return ObjectInspectorFactory.getStandardStructObjectInspector(
                        Arrays.asList("row","matrix"),fieldOIs);
            } else {
                return ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
            }
        }

        @Override
        public AbstractAggregationBuffer getNewAggregationBuffer() throws HiveException {
            ColumnDotArrayAggregationBuffer agg = new ColumnDotArrayAggregationBuffer();
            reset(agg);
            return agg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ColumnDotArrayAggregationBuffer myagg = (ColumnDotArrayAggregationBuffer) agg;
            myagg.reset();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters[0] == null || parameters[1] == null) {
                return;
            }

            ColumnDotArrayAggregationBuffer myagg = (ColumnDotArrayAggregationBuffer) agg;

            if (myagg.row == null) {
                List rowList = rowOI.getList(parameters[1]);
                int length = rowList.size();
                double[] row = new double[length];
                for (int i = 0; i < length; i++) {
                    row[i] = rowElOI.get(rowList.get(i));
                }
                myagg.row = row;
            }

            double val = colOI.get(parameters[0]);
            double[] newRow = new double[myagg.row.length];
            for(int i=0;i<myagg.row.length;i++){
                newRow[i]=myagg.row[i]*val;
            }
            myagg.matrix.add(newRow);
        }

        @Override
        public void merge(AggregationBuffer agg, Object other) throws HiveException {
            ColumnDotArrayAggregationBuffer myagg = (ColumnDotArrayAggregationBuffer) agg;

            List rowList = rowOI.getList(structOI.getStructFieldData(other,rowField));
            List matrixList= matrixOI.getList(structOI.getStructFieldData(other,matrixField));

            double[] row =new double[rowList.size()];
            for(int i=0;i<rowList.size();i++){
                row[i]=rowElOI.get(rowList.get(i));
            }
            myagg.row=row;

            for(Object matrixRowObj:matrixList){
                List matrixRow = matrixRowOI.getList(matrixRowObj);
                double[] ds = new double[myagg.row.length];
                for(int i=0;i<myagg.row.length;i++){
                    ds[i]=matrixRowElOI.get( matrixRow.get(i));
                }
                myagg.matrix.add(ds); // TODO: order
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            ColumnDotArrayAggregationBuffer myagg = (ColumnDotArrayAggregationBuffer) agg;
            Object[] partialResult = new Object[2];
            List<DoubleWritable> row = new ArrayList<DoubleWritable>();
            List<List<DoubleWritable>> matrix = new ArrayList<List<DoubleWritable>>();
            for(double d: myagg.row){
                row.add(new DoubleWritable(d));
            }
            for(double[] ds:myagg.matrix){
                List<DoubleWritable> newRow = new ArrayList<DoubleWritable>();
                for(double d:ds){
                    newRow.add(new DoubleWritable(d));
                }
                matrix.add(newRow);
            }

            partialResult[0]=row;
            partialResult[1]=matrix;
            return partialResult;
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ColumnDotArrayAggregationBuffer myagg = (ColumnDotArrayAggregationBuffer) agg;
            List<List<DoubleWritable>> result = new ArrayList<List<DoubleWritable>>();
            for(int i=0;i<myagg.matrix.size();i++){
                List<DoubleWritable> newRow = new ArrayList<DoubleWritable>();
                for(double d:myagg.matrix.get(i)){
                    newRow.add(new DoubleWritable(d));
                }
                result.add(newRow);
            }
            return result;
        }
    }
}
