/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.ftvec.selection;

import hivemall.utils.hadoop.HiveUtils;
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
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

@Description(
        name = "array_sum_of_each_class",
        value = "_FUNC_(number label, array<number> arr)"
                + " - Returns sum of elements each class as array<array<double>>, shape = (#classes, #lenOfArray)")
public class ArraySumOfEachClassUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
            throws SemanticException {
        ObjectInspector[] OIs = info.getParameterObjectInspectors();

        if (OIs.length != 2) {
            throw new UDFArgumentLengthException("Specify two arguments.");
        }

        if (!HiveUtils.isPrimitiveOI(OIs[0])) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
                    + OIs[0].getTypeName() + " was passed as `label`");
        }

        if (!HiveUtils.isListOI(OIs[1])
                || !HiveUtils.isNumberOI(((ListObjectInspector) OIs[1]).getListElementObjectInspector())) {
            throw new UDFArgumentTypeException(1,
                "Only array<number> type arguments are accepted but " + OIs[1].getTypeName()
                        + " was passed as `arr`");
        }

        return new ArraySumOfEachClassUDAFEvaluator();
    }

    private static class ArraySumOfEachClassUDAFEvaluator extends GenericUDAFEvaluator {
        // PARTIAL1 and COMPLETE
        private PrimitiveObjectInspector labelOI;
        private ListObjectInspector arrayOI;
        private DoubleObjectInspector sumOI;

        // PARTIAL2 and FINAL
        private StructObjectInspector structOI;
        private StructField sumOfEachClassField, lenOfArrayField;
        private MapObjectInspector sumOfEachClassOI;
        private DoubleObjectInspector classOI;
        private ListObjectInspector sumArrayOI;
        private DoubleObjectInspector sumArrayElOI;
        private IntObjectInspector lenOfArrayOI;

        @AggregationType(estimable = true)
        static class ArraySumOfEachClassAggregationBuffer extends AbstractAggregationBuffer {
            int lenOfArray;
            HashMap<Double, Double[]> sumOfEachClass; // k: class, v: sumOfEachClass of each features

            @Override
            public int estimate() {
                return sumOfEachClass.size() * 8 * 4; // lenOfArray
            }

            public void reset() {
                lenOfArray = -1;

                if (sumOfEachClass == null) {
                    sumOfEachClass = new HashMap<Double, Double[]>();
                } else {
                    sumOfEachClass.clear(); // TODO: reuse arrays by zero fill(?)
                }
            }
        }

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] OIs) throws HiveException {
            super.init(mode, OIs);

            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                labelOI = HiveUtils.asDoubleCompatibleOI(OIs[0]);
                arrayOI = (StandardListObjectInspector) OIs[1];
                sumOI = (DoubleObjectInspector) arrayOI.getListElementObjectInspector();
            } else {
                structOI = (StandardStructObjectInspector) OIs[0];
                sumOfEachClassField = structOI.getStructFieldRef("sumOfEachClass");
                lenOfArrayField = structOI.getStructFieldRef("lenOfArray");
                sumOfEachClassOI = (StandardMapObjectInspector) sumOfEachClassField.getFieldObjectInspector();
                classOI = (DoubleObjectInspector) sumOfEachClassOI.getMapKeyObjectInspector();
                sumArrayOI = (StandardListObjectInspector) sumOfEachClassOI.getMapValueObjectInspector();
                sumArrayElOI = (DoubleObjectInspector) sumArrayOI.getListElementObjectInspector();
                lenOfArrayOI = (IntObjectInspector) lenOfArrayField.getFieldObjectInspector();
            }

            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                List<String> fieldNames = new ArrayList<String>();
                List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
                fieldNames.add("sumOfEachClass");
                fieldOIs.add(ObjectInspectorFactory.getStandardMapObjectInspector(
                    PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                    ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)));
                fieldNames.add("lenOfArray");
                fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

                return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
            } else {
                return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
            }
        }

        @Override
        public AbstractAggregationBuffer getNewAggregationBuffer() throws HiveException {
            ArraySumOfEachClassAggregationBuffer agg = new ArraySumOfEachClassAggregationBuffer();
            reset(agg);
            return agg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ArraySumOfEachClassAggregationBuffer myagg = (ArraySumOfEachClassAggregationBuffer) agg;
            myagg.reset();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters[0] == null || parameters[1] == null) {
                return;
            }

            ArraySumOfEachClassAggregationBuffer myagg = (ArraySumOfEachClassAggregationBuffer) agg;
            double clazz = PrimitiveObjectInspectorUtils.getDouble(parameters[0], labelOI);
            double[] array = HiveUtils.asDoubleArray(parameters[1], arrayOI, sumOI);

            if (array == null) {
                return;
            }

            if (myagg.lenOfArray == -1) {
                myagg.lenOfArray = array.length;
            } else {
                Preconditions.checkArgument(myagg.lenOfArray == array.length,
                    "Input array was illegal.");
            }

            if (myagg.sumOfEachClass.containsKey(clazz)) {
                Double[] accArray = myagg.sumOfEachClass.get(clazz);
                for (int i = 0; i < myagg.lenOfArray; i++) {
                    accArray[i] += array[i];
                }
            } else {
                Double[] accArray = new Double[myagg.lenOfArray];
                for (int i = 0; i < myagg.lenOfArray; i++) {
                    accArray[i] = array[i];
                }
                myagg.sumOfEachClass.put(clazz, accArray);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object other) throws HiveException {
            if (other == null) {
                return;
            }

            ArraySumOfEachClassAggregationBuffer myagg = (ArraySumOfEachClassAggregationBuffer) agg;
            myagg.lenOfArray = lenOfArrayOI.get(structOI.getStructFieldData(other, lenOfArrayField));
            Map sumOfEachClass = sumOfEachClassOI.getMap(structOI.getStructFieldData(other,
                sumOfEachClassField));
            for (Object key : sumOfEachClass.keySet()) {
                double clazz = classOI.get(key);
                double[] array = HiveUtils.asDoubleArray(sumOfEachClass.get(key), sumArrayOI,
                    sumArrayElOI);
                if (array == null) {
                    return;
                }

                if (!myagg.sumOfEachClass.containsKey(clazz)) {
                    Double[] accArray = new Double[myagg.lenOfArray];
                    for (int i = 0; i < myagg.lenOfArray; i++) {
                        accArray[i] = array[i];
                    }
                    myagg.sumOfEachClass.put(clazz, accArray);
                } else {
                    Double[] accArray = myagg.sumOfEachClass.get(clazz);
                    for (int i = 0; i < myagg.lenOfArray; i++) {
                        accArray[i] += array[i];
                    }
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            ArraySumOfEachClassAggregationBuffer myagg = (ArraySumOfEachClassAggregationBuffer) agg;
            Map<DoubleWritable, List<DoubleWritable>> sumOfEachClass = new HashMap<DoubleWritable, List<DoubleWritable>>();
            for (Map.Entry<Double, Double[]> e : myagg.sumOfEachClass.entrySet()) {
                List<DoubleWritable> array = new ArrayList<DoubleWritable>();
                for (double d : e.getValue()) {
                    array.add(new DoubleWritable(d));
                }
                sumOfEachClass.put(new DoubleWritable(e.getKey()), array);
            }
            return new Object[] {sumOfEachClass, new IntWritable(myagg.lenOfArray)};
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ArraySumOfEachClassAggregationBuffer myagg = (ArraySumOfEachClassAggregationBuffer) agg;
            List<List<DoubleWritable>> result = new ArrayList<List<DoubleWritable>>();
            for (Double key : new TreeSet<Double>(myagg.sumOfEachClass.keySet())) {
                List<DoubleWritable> array = new ArrayList<DoubleWritable>();
                for (double d : myagg.sumOfEachClass.get(key)) {
                    array.add(new DoubleWritable(d));
                }
                result.add(array);
            }
            return result;
        }
    }
}
