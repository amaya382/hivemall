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
package hivemall.tools.count;

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
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

@Description(name = "occurrence_prob",
        value = "_FUNC_(label) - Returns occurrence probability as array<double>")
public class OccurrenceProbUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
            throws SemanticException {
        ObjectInspector[] OIs = info.getParameterObjectInspectors();

        if (OIs.length != 1) {
            throw new UDFArgumentLengthException("Specify one argument.");
        }

        if (!HiveUtils.isPrimitiveOI(OIs[0])) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
                    + OIs[0].getTypeName() + " was passed as `label`");
        }

        return new OccurrenceProbGenericUDAFEvaluator();
    }

    private static class OccurrenceProbGenericUDAFEvaluator extends GenericUDAFEvaluator {
        // PARTIAL1 and COMPLETE
        private PrimitiveObjectInspector labelOI;

        // PARTIAL2 and FINAL
        private MapObjectInspector countTableOI;
        private StringObjectInspector classOI;
        private LongObjectInspector countOI;

        @AggregationType(estimable = true)
        static class OccurrenceProbAggregationBuffer extends AbstractAggregationBuffer {
            Map<String, Long> countTable;

            @Override
            public int estimate() {
                return countTable.size() * 8; // estimating value for size of *this* buffer
            }

            public void reset() {
                if (countTable == null) {
                    countTable = new HashMap<String, Long>();
                } else {
                    countTable.clear(); // TODO: zero fill(?)
                }
            }
        }

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] OIs) throws HiveException {
            super.init(mode, OIs);

            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                labelOI = (PrimitiveObjectInspector) OIs[0];
            } else {
                countTableOI = (MapObjectInspector) OIs[0];
                classOI = (StringObjectInspector) countTableOI.getMapKeyObjectInspector();
                countOI = (LongObjectInspector) countTableOI.getMapValueObjectInspector();
            }

            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                return ObjectInspectorFactory.getStandardMapObjectInspector(
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                    PrimitiveObjectInspectorFactory.writableLongObjectInspector);
            } else {
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
            }
        }

        @Override
        public AbstractAggregationBuffer getNewAggregationBuffer() throws HiveException {
            OccurrenceProbAggregationBuffer myAgg = new OccurrenceProbAggregationBuffer();
            reset(myAgg);
            return myAgg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            OccurrenceProbAggregationBuffer myAgg = (OccurrenceProbAggregationBuffer) agg;
            myAgg.reset();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters[0] == null) {
                return;
            }

            OccurrenceProbAggregationBuffer myAgg = (OccurrenceProbAggregationBuffer) agg;
            String clazz = String.valueOf(labelOI.getPrimitiveJavaObject(parameters[0]));

            myAgg.countTable.put(clazz,
                myAgg.countTable.containsKey(clazz) ? myAgg.countTable.get(clazz) + 1L : 1L);
        }

        @Override
        public void merge(AggregationBuffer agg, Object other) throws HiveException {
            if (other == null) {
                return;
            }

            OccurrenceProbAggregationBuffer myAgg = (OccurrenceProbAggregationBuffer) agg;
            Map countTable = countTableOI.getMap(other);
            for (Object key : countTable.keySet()) {
                String clazz = classOI.getPrimitiveJavaObject(key);
                myAgg.countTable.put(clazz,
                        countOI.get(countTable.get(key))+
                        (myAgg.countTable.containsKey(clazz) ? myAgg.countTable.get(clazz) + 1L : 1L));
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            OccurrenceProbAggregationBuffer myAgg = (OccurrenceProbAggregationBuffer) agg;
            Map<Text, LongWritable> countTable = new HashMap<Text, LongWritable>();
            for (Map.Entry<String, Long> e : myAgg.countTable.entrySet()) {
                countTable.put(new Text(e.getKey()), new LongWritable(e.getValue()));
            }
            return countTable;
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            OccurrenceProbAggregationBuffer myAgg = (OccurrenceProbAggregationBuffer) agg;

            long all = 0L;
            List<DoubleWritable> result = new ArrayList<DoubleWritable>();
            // TODO: order or sth
            for (String key : new TreeSet<String>(myAgg.countTable.keySet())) {
                Long count = myAgg.countTable.get(key);
                all += count;
                result.add(new DoubleWritable(count.doubleValue()));
            }
            for (DoubleWritable d : result) {
                d.set(d.get() / all);
            }

            return result;
        }
    }
}
