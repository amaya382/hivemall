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
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

@Description(name = "array_dot_array",
        value = "_FUNC_(array<number> array0, array<number> array1) - Returns matrix as array<array<double>>")
public class ArrayDotArrayUDF extends GenericUDF {
    private ListObjectInspector vector0OI;
    private ListObjectInspector vector1OI;
    private DoubleObjectInspector vector0ElOI;
    private DoubleObjectInspector vector1ElOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {
        if (OIs.length != 2) {
            throw new UDFArgumentLengthException("Specify two arguments.");
        }

        if (!HiveUtils.isListOI(OIs[0])||
                !HiveUtils.isNumberOI(((ListObjectInspector)OIs[0]).getListElementObjectInspector())) {
            throw new UDFArgumentTypeException(0, "Only array<number> type argument is acceptable but "
                    + OIs[0].getTypeName() + " was passed as `array0`");
        }
        if (!HiveUtils.isListOI(OIs[1])||
                !HiveUtils.isNumberOI(((ListObjectInspector)OIs[1]).getListElementObjectInspector())) {
            throw new UDFArgumentTypeException(1, "Only array<number> type argument is acceptable but "
                    + OIs[1].getTypeName() + " was passed as `array1`");
        }

        vector0OI=(ListObjectInspector)OIs[0];
        vector1OI=(ListObjectInspector)OIs[1];
        vector0ElOI=(DoubleObjectInspector)vector0OI.getListElementObjectInspector();
        vector1ElOI=(DoubleObjectInspector)vector1OI.getListElementObjectInspector();

        return ObjectInspectorFactory.getStandardListObjectInspector(
                ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] dObj) throws HiveException {
        List vector0 = vector0OI.getList(dObj[0].get());
        List vector1 = vector1OI.getList(dObj[1].get());

        List<List<DoubleWritable>> result = new ArrayList<List<DoubleWritable>>();
        for(Object o1:vector1){
            List<DoubleWritable> row = new ArrayList<DoubleWritable>();
            for(Object o0: vector0){
                row.add(new DoubleWritable(
                        vector0ElOI.get(o0)*vector1ElOI.get(o1)));
            }
            result.add(row);
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("array_dot_array");
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
