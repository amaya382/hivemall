package hivemall.tools.matrix;

import hivemall.utils.hadoop.HiveUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

@Description(name = "transposeOne",
        value = "_FUNC_(array<array<number|string>> matrix) - Returns transposed matrix as array<array<double|string>>")
public class TransposeOneUDF extends GenericUDF {
    private ListObjectInspector matrixOI;
    private ListObjectInspector rowOI;
    private PrimitiveObjectInspector elementOI;

    private boolean isNumeric;

    @Override
    public ObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {
        if (OIs.length != 1) {
            throw new UDFArgumentLengthException("Specify one argument.");
        }

        matrixOI=HiveUtils.asListOI(OIs[0]);
        rowOI=HiveUtils.asListOI(matrixOI.getListElementObjectInspector());

        ObjectInspector _elementOI = rowOI.getListElementObjectInspector();
        if(HiveUtils.isNumberOI(_elementOI)){
            isNumeric =true;
            elementOI=HiveUtils.asDoubleCompatibleOI(_elementOI);
        }else if(HiveUtils.isStringOI(_elementOI)) {
            elementOI = (StringObjectInspector) _elementOI;
        }else{
            throw new UDFArgumentTypeException(0, "Only array<array<number|string>> type arguments is acceptable but "
                    + OIs[0].getTypeName() + " was passed as `matrix`");
        }

        return isNumeric
                ? ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorFactory.getStandardListObjectInspector(
                                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector))
                :ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorFactory.getStandardListObjectInspector(
                                PrimitiveObjectInspectorFactory.writableStringObjectInspector));
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] dObj) throws HiveException {
        List matrix = matrixOI.getList(dObj[0].get());

        if(isNumeric){
            List<List<DoubleWritable>> result = new ArrayList<List<DoubleWritable>>();
            for(int i=0;i<rowOI.getListLength(matrix.get(0));i++){
                result.add(new ArrayList<DoubleWritable>());
            }

            for (Object rowObj : matrix) {
                List row = rowOI.getList(rowObj);
                for (int j = 0; j < row.size(); j++) {
                    result.get(j).add(
                            new DoubleWritable(PrimitiveObjectInspectorUtils.getDouble(row.get(j),elementOI)));
                }
            }
            return result;
        }else{
            List<List<Text>> result = new ArrayList<List<Text>>();
            for(int i=0;i<rowOI.getListLength(matrix.get(0));i++){
                result.add(new ArrayList<Text>());
            }

            for (Object rowObj : matrix) {
                List row = rowOI.getList(rowObj);
                for (int j = 0; j < row.size(); j++) {
                    result.get(j).add(
                            new Text(PrimitiveObjectInspectorUtils.getString(row.get(j),elementOI)));
                }
            }
            return result;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("transposeOne");
        sb.append("(");
        if (children.length > 0) {
            sb.append(children[0]);
            for (int i = 1; i < children.length; i++) {
                sb.append(", ");
                sb.append(children[i]);
            }
        }
        sb.append(")");
        return sb.toString();
    }
}
