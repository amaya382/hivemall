package hivemall.ftvec.selection;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Preconditions;
import hivemall.utils.math.StatsUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import javax.annotation.Nonnull;

@Description(name = "",
        value = "_FUNC_(array<number> expected, array<number> observed) - Returns dissociation degree as double")
abstract class DissociationDegreeUDF extends GenericUDF {
    private ListObjectInspector expectedOI;
    private PrimitiveObjectInspector expectedElOI;
    private ListObjectInspector observedOI;
    private PrimitiveObjectInspector observedElOI;

    private double[] expected;
    private double[] observed;

    @Override
    public ObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {
        if (OIs.length != 2) {
            throw new UDFArgumentLengthException("Specify two arguments.");
        }

        if (!HiveUtils.isNumberListOI(OIs[0])){
            throw new UDFArgumentTypeException(0, "Only array<number> type argument is acceptable but "
                    + OIs[0].getTypeName() + " was passed as `expected`");
        }

        if (!HiveUtils.isNumberListOI(OIs[1])){
            throw new UDFArgumentTypeException(1, "Only array<number> type argument is acceptable but "
                    + OIs[1].getTypeName() + " was passed as `observed`");
        }

        expectedOI =  HiveUtils.asListOI(OIs[0]);
        expectedElOI = HiveUtils.asDoubleCompatibleOI(expectedOI.getListElementObjectInspector());
        observedOI = HiveUtils.asListOI(OIs[1]);
        observedElOI = HiveUtils.asDoubleCompatibleOI( observedOI.getListElementObjectInspector());

        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] dObj) throws HiveException {
        if(expected==null){
           expected=new double[ expectedOI.getListLength(dObj[0].get())];
        }
        if(observed==null){
            observed=new double[ observedOI.getListLength(dObj[1].get())];
        }
        Preconditions.checkArgument(expected.length == observed.length);

        HiveUtils.toDoubleArray(dObj[0].get(),expectedOI,expectedElOI,expected,false);
        HiveUtils.toDoubleArray(dObj[1].get(),observedOI,observedElOI,observed, false);

        return new DoubleWritable(calcDissociation(expected,observed));
    }

    @Override
    public String getDisplayString(String[] children) {
        final StringBuilder sb = new StringBuilder();
        sb.append(getFuncName());
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

    abstract double calcDissociation(@Nonnull final double[] expected,@Nonnull final  double[] observed);

    @Nonnull
    abstract String getFuncName();
}
