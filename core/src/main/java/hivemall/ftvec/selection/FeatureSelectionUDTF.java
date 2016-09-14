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

import hivemall.utils.collections.IntOpenHashMap;
import hivemall.utils.hadoop.HiveUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Description(
        name = "feature_selection",
        value = "_FUNC_(array<features::string> features, const string target_label, const int num_of_features[, const string method = chi2])")
public class FeatureSelectionUDTF extends GenericUDTF {
    private ListObjectInspector featuresOI;
    private StringObjectInspector featureOI;
    private int numOfFeatures;
    private String targetLabel;
    private String method = "chi2";

    private IntOpenHashMap<Double[]> observed;
    private Double[] featureCount;
    private Double[] classProb;

    private static final int nFeatures = 4;
    private static final int nClasses = 3;
    private static final int nSamples = 150;


    List<List<Double>> records = new ArrayList<List<Double>>();
    List<List<Double>> featureBuckets = new ArrayList<List<Double>>();
    List<Double> labelBuckets = new ArrayList<Double>();



    @Override
    public StructObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {
        if (OIs.length != 3 && OIs.length != 4) {
            throw new UDFArgumentLengthException("Specify three or four arguments");
        }

        featuresOI = HiveUtils.asListOI(OIs[0]);
        featureOI = HiveUtils.asStringOI(featuresOI.getListElementObjectInspector());
        targetLabel = HiveUtils.getConstString(OIs[1]);
        numOfFeatures = HiveUtils.getAsConstInt(OIs[2]);
        if (OIs.length == 4) {
            method = HiveUtils.getConstString(OIs[3]);
        }



        observed = new IntOpenHashMap<Double[]>(nClasses);
        for (int i = 0; i < nClasses; i++) {
            Double[] arr = new Double[nFeatures];
            Arrays.fill(arr, 0.0);
            observed.put(i, arr);
        }

        featureCount = new Double[nFeatures];
        classProb = new Double[nClasses]; // need divide by #samples
        Arrays.fill(featureCount, 0.0);
        Arrays.fill(classProb, 0.0);

        final List<String> fieldNames = new ArrayList<String>();
        final List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("col1dayo");
        fieldNames.add("col2dayo");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objs) throws HiveException {
        //        forward(new Object[]{"aaa", "nnn"});

        /*
        List features = featuresOI.getList(objs[0]);
        int hot=-1;
        double[] _features = new double[lenOfArray];
        Arrays.fill(_features, 0.0);
        for (int i = 0, j = 0; i < lenOfArray + 1; i++) {
            String str = String.valueOf(features.get(i));
            boolean isCate =str.indexOf("#")>0;
                String name = str.substring(0,
                        isCate
                        ?str.indexOf("#")
                        :str.indexOf(":"));
            if (name.equals(targetLabel)) {
                hot=Integer.valueOf(str.substring((isCate?str.indexOf("#"):str.indexOf(":"))+1,str.length()));
                classProb[hot]++;
            } else {
                double val = Double.valueOf(
                        str.substring(str.indexOf(":")+1,str.length()));
                featureCount[j] += val;
                _features[j] += val;
                j++;
            }
        }

        Double[] active = observed.get(hot);
        for(int i=0;i<active.length;i++){
            active[i]+=_features[i];
        }
        */

        List features = featuresOI.getList(objs[0]);
        List _features = features.subList(1, features.size() - 1);
        List<Double> res = new ArrayList<Double>();
        for (Object o : _features) {
            String str = o.toString();
            String val = str.substring(
                str.indexOf("#") > 0 ? str.indexOf("#") + 1 : str.indexOf(":") + 1, str.length());
            res.add(Double.valueOf(val));
        }

        records.add(res);
        String label = features.get(0).toString();
        labelBuckets.add(Double.valueOf(label.substring(label.indexOf("#") + 1, label.length())));
    }

    @Override
    public void close() throws HiveException {
        //        forward(new Object[]{"ccc", "ddd"}); // x

        /*
        for(int i=0;i<nClasses;i++){
            classProb[i]/=nSamples;
        }

        ChiSquareTest chi2 = new ChiSquareTest();
        List<Double> result0 = new ArrayList<Double>();
        List<Double> result1 = new ArrayList<Double>();

        for(int i=0;i<lenOfArray;i++) {
            long[] longs = new long[nClasses];
            double[] doubles = new double[nClasses];
            for(int j=0;j<nClasses;j++){
                longs[j]=observed.get(j)[i].longValue();
                doubles[j] += classProb[j]*featureCount[i];
            }

            result0.add(chi2.chiSquareTest(doubles, longs));
            result1.add(chi2.chiSquare(doubles, longs));
        }

        System.out.println(result0);
        System.out.println(result1);
        */


        for (int j = 0; j < nFeatures; j++) {
            List<Double> inner = new ArrayList<Double>();
            for (int k = 0; k < nSamples; k++) {
                inner.add(0.0);
            }
            featureBuckets.add(inner);
        }

        for (int i = 0; i < records.size(); i++) {
            for (int j = 0; j < records.get(i).size(); j++) {
                featureBuckets.get(j).set(i, records.get(i).get(j));
            }
        }

        List<Integer> sele = mRMR();
        System.out.print(sele);
    }


    private List<Integer> mRMR() {

        LinkedHashMap<Map.Entry<Integer, Integer>, Double> infoGainBetweenFeatures = new LinkedHashMap<Map.Entry<Integer, Integer>, Double>();

        boolean[] selected = new boolean[nFeatures];
        Arrays.fill(selected, false);


        List<Integer> indices = new ArrayList<Integer>();
        for (int i = 0; i < numOfFeatures; i++) {
            indices.add(i);
        }

        List<Map.Entry<Double, Integer>> internalRR = new ArrayList<Map.Entry<Double, Integer>>();

        List<Double> infoGainWithLabels = infoGain(indices, labelBuckets, -1);
        Map.Entry<Double, Integer> maxWithIdx = maxOf(infoGainWithLabels);
        selected[maxWithIdx.getValue()] = true;
        internalRR.add(maxWithIdx);

        Integer lastSelected = maxWithIdx.getValue();
        for (int i = 0; i < numOfFeatures - 1/*init feature*/; i++) {
            List<Integer> notSelected = new ArrayList<Integer>();
            for (int j = 0; i < selected.length; j++) {
                if (!selected[j]) {
                    notSelected.add(j);
                }
            }

            List<Double> newInfoGain = infoGain(notSelected, records.get(lastSelected),
                lastSelected);
            Integer nSelected = nFeatures - notSelected.size();

            Map.Entry<Double, Integer> max = new AbstractMap.SimpleEntry<Double, Integer>(0.0, 0);
            for (int j = 0; j < notSelected.size(); j++) {
                infoGainWithLabels.get(j);
                double agg = 0.0;
                for (Map.Entry<Map.Entry<Integer, Integer>, Double> e : infoGainBetweenFeatures.entrySet()) {
                    Map.Entry<Integer, Integer> idxPair = e.getKey();
                    if ((idxPair.getKey() == j && selected[idxPair.getValue()])
                            || idxPair.getValue() == j && selected[idxPair.getKey()]) {
                        agg += e.getValue();
                    }
                }
                agg /= nSelected;
                if (max.getValue() < agg) {
                    max = new AbstractMap.SimpleEntry<Double, Integer>(agg, j);
                }
            }
            selected[max.getValue()] = true;
            lastSelected = max.getValue();
            internalRR.add(max);

            for (int j = 0; j < newInfoGain.size(); j++) {
                infoGainBetweenFeatures.put(new AbstractMap.SimpleEntry<Integer, Integer>(j,
                    lastSelected), newInfoGain.get(j));
            }
        }

        List<Integer> selectedIdx = new ArrayList<Integer>();
        for (int i = 0; i < selected.length; i++) {
            if (selected[i]) {
                selectedIdx.add(i);
            }
        }
        return selectedIdx;
    }

    private Map.Entry<Double, Integer> maxOf(List<Double> list) {
        Double max = Double.NEGATIVE_INFINITY;
        int maxIdx = 0;
        for (int i = 0; i < list.size(); i++) {
            if (max < list.get(i)) {
                max = list.get(i);
                maxIdx = i;
            }
        }
        return new AbstractMap.SimpleEntry<Double, Integer>(max, maxIdx);
    }

    private List<Double> infoGain(List<Integer> featureIndices, List<Double> buckets, int lastS) {
        List<List<List<Integer>>> allInitBuckets = new ArrayList<List<List<Integer>>>(
            featureIndices.size());
        for (int i = 0; i < featureIndices.size(); i++) {
            List<List<Integer>> inner = new ArrayList<List<Integer>>();
            for (int j = 0; j < buckets.size(); j++) {
                List<Integer> innerInner = new ArrayList<Integer>();
                for (int k = 0; k < featureBuckets.get(featureIndices.get(i)).size(); k++) {
                    innerInner.add(0);
                }
                inner.add(innerInner);
            }
            allInitBuckets.add(inner);
        }


        //        List<List<Double>> featureBuckets = new ArrayList<List<Double>>();

        for (int i = 0; i < records.size(); i++) {
            for (int j = 0; j < featureIndices.size(); j++) {
                int targetIdx = Arrays.binarySearch(buckets.toArray(new Double[0]), lastS == -1 ? j
                        : records.get(i).get(lastS));
                int featureIdx = Arrays.binarySearch(featureBuckets.get(featureIndices.get(j))
                                                                   .toArray(new Double[0]),
                    records.get(i).get(featureIndices.get(j)));
                allInitBuckets.get(j)
                              .get(targetIdx)
                              .set(featureIdx,
                                  allInitBuckets.get(j).get(targetIdx).get(featureIdx) + 1);
            }
        }

        List<Integer> targetBucketValues = new ArrayList<Integer>();
        for (int i = 0; i < allInitBuckets.get(0).size(); i++) {
            Integer sum = 0;
            for (Integer val : allInitBuckets.get(0).get(i)) {
                sum += val;
            }
            targetBucketValues.add(sum);
        }
        List<List<Integer>> featureBucketValues = new ArrayList<List<Integer>>();
        for (int i = 0; i < allInitBuckets.size(); i++) {
            List<Integer> inner = new ArrayList<Integer>();
            for (int j = 0; j < allInitBuckets.get(i).size(); j++) {
                Integer agg = 0;
                for (int k = 0; k < allInitBuckets.size(); k++) {
                    agg += allInitBuckets.get(i).get(k).get(j);
                }
                inner.add(agg);
            }
            featureBucketValues.add(inner);
        }

        List<Double> result = new ArrayList<Double>();
        for (int i = 0; i < featureIndices.size(); i++) {
            result.add(Double.NaN);
        }

        for (int i = 0; i < allInitBuckets.size(); i++) {
            List<List<Integer>> featureValues = allInitBuckets.get(i);
            double agg = 0.0;
            for (int j = 0; j < featureValues.size(); j++) {
                for (int k = 0; k < featureValues.get(j).size(); k++) {
                    int x = featureValues.get(j).get(k);
                    agg += x > 0 ? (double) x
                            * Math.log(x)
                            * (double) nSamples
                            / (double) (targetBucketValues.get(j) * featureBucketValues.get(i).get(
                                k)) : 0.0;

                }
            }
            result.set(i, agg);
        }

        return result;
    }

    /*
    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("feature_selection");
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
    */
}
