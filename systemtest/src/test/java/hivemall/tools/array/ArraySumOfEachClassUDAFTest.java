package hivemall.tools.array;

import hivemall.TestOfTest;
import hivemall.systemtest.model.HQ;
import hivemall.systemtest.runner.HiveSystemTestRunner;
import hivemall.systemtest.runner.SystemTestCommonInfo;
import hivemall.systemtest.runner.SystemTestTeam;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

public class ArraySumOfEachClassUDAFTest {
    private static SystemTestCommonInfo ci = new SystemTestCommonInfo(ArraySumOfEachClassUDAFTest.class);

    @ClassRule
    public static HiveSystemTestRunner hRunner = new HiveSystemTestRunner(ci) {
        {
            /*
            initBy(HQ.uploadByResourcePathAsNewTable("iris", ci.initDir + "iris.csv",
                    new LinkedHashMap<String, String>() {
                        {
                            put("a", "double");
                            put("b", "double");
                            put("c", "double");
                            put("d", "double");
                            put("label", "string");
                        }
                    }));
                    */
            initBy(HQ.uploadByResourcePathAsNewTable("iris0", ci.initDir + "iris0.csv",
                    new LinkedHashMap<String, String>() {
                        {
                            put("a", "double");
                            put("b", "double");
                            put("c", "double");
                            put("d", "double");
                            put("c0", "int");
                            put("c1", "int");
                            put("c2", "int");
                        }
                    }));
            /*
            initBy(HQ.uploadByResourcePathAsNewTable("iris00", ci.initDir + "iris00.csv",
                    new LinkedHashMap<String, String>() {
                        {
                            put("X", "array<double>");
                            put("Y", "array<int>");
                        }
                    }));
                    */
            initBy(HQ.fromStatements("" +
                    "CREATE TEMPORARY FUNCTION quantify as 'hivemall.ftvec.conv.QuantifyColumnsUDTF';" +
                    "CREATE TEMPORARY FUNCTION array_sum_of_each_class as 'hivemall.tools.array.ArraySumOfEachClassUDAF';" +
                    "CREATE TEMPORARY FUNCTION array_sum as 'hivemall.tools.array.ArraySumUDAF';" +
                    "CREATE TEMPORARY FUNCTION occurrence_prob as 'hivemall.tools.count.OccurrenceProbUDAF';" +
                    "CREATE TEMPORARY FUNCTION column_dot_array as 'hivemall.tools.matrix.ColumnDotArrayUDAF';" +
                    "CREATE TEMPORARY FUNCTION vector_dot_vector as 'hivemall.tools.matrix.VectorDotVectorUDF';" +
                    "CREATE TEMPORARY FUNCTION vector_dot_vector as 'hivemall.tools.matrix.VectorDotVectorUDF';" +
                    "CREATE TEMPORARY FUNCTION transposeOne as 'hivemall.tools.matrix.TransposeOneUDF';" +
                    "CREATE TEMPORARY FUNCTION chi2 as 'hivemall.ftvec.selection.ChiSquareUDF';" +
                    "CREATE TEMPORARY FUNCTION array_avg as 'hivemall.tools.array.ArrayAvgGenericUDAF';" +
                    "CREATE TEMPORARY FUNCTION transpose_and_dot as 'hivemall.tools.matrix.TransposeAndDotUDAF';" +
                    "CREATE TEMPORARY FUNCTION select_k_best as 'hivemall.tools.array.SelectKBestUDF';" +
                    "CREATE TEMPORARY FUNCTION maxrow as 'hivemall.ensemble.MaxRowUDAF';" +
                    "CREATE TEMPORARY FUNCTION snr as 'hivemall.ftvec.selection.SignalNoiseRatioUDAF';" +
                    ""));
        }
    };

    @Rule
    public SystemTestTeam team = new SystemTestTeam(hRunner);


    @Test
    public void payoo() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "WITH iris AS (" +
                "  SELECT array(a, b, c, d) AS X, array(c0, c1, c2) AS Y FROM iris0" +
                ")" +
                "SELECT snr(X, Y)" +
                "FROM iris"));
        for(String s:res){
            System.out.println(s);
        }
    }


    @Test
    public void piyo() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "WITH iris AS (" +
                "  SELECT array(a, b, c, d) AS X, array(c0, c1, c2) AS Y FROM iris0" +
                ")" +
                "SELECT snr(X, Y)" +
                "FROM iris"));
        for(String s:res){
            System.out.println(s);
        }
    }

    @Test
    public void poyo0() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "WITH iris AS (" +
                "  SELECT array(a, b, c, d) AS X, array(c0, c1, c2) AS Y FROM iris0)," +
                "stats AS (" +
                "  SELECT" +
                "    transpose_and_dot(Y, X) AS observed," +
                "    array_sum(X) AS feature_count," +
                "    array_avg(Y) AS class_prob" +
                "  FROM" +
                "    iris)," +
                "test AS (" +
                "  SELECT" +
                "    transpose_and_dot(class_prob, feature_count) AS expected" +
                "  FROM" +
                "    stats)" +
                "SELECT" +
                "  chi2(observed, expected) AS x " +
                "FROM" +
                "  test JOIN stats" +
                ""));
        for(String s:res){
            System.out.println(s);
        }
    }

    @Test
    public void poyo05() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "WITH iris AS (" +
                "  SELECT array(a, b, c, d) AS X, array(c0, c1, c2) AS Y FROM iris0)," +
                "stats AS (" +
                "  SELECT" +
                "    transpose_and_dot(Y, X) AS observed," +
                "    array_sum(X) AS feature_count," +
                "    array_avg(Y) AS class_prob" +
                "  FROM" +
                "    iris)," +
                "test AS (" +
                "  SELECT" +
                "    transpose_and_dot(class_prob, feature_count) AS expected" +
                "  FROM" +
                "    stats)," +
                "aa AS (" +
                "  SELECT" +
                "    chi2(observed, expected) AS x" +
                "  FROM" +
                "    test JOIN stats)," +
                "bb AS (" +
                "  SELECT sort_array(array_top_k_indices(x.chi2_vals, 3)) AS yy FROM aa" +
                ")" +
                "SELECT select_by_index(X, yy) FROM iris LEFT JOIN bb" +
                ""));
        for(String s:res){
            System.out.println(s);
        }
    }

    @Test
    public void hoge2() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "SELECT quantify(array(true, a, b, c, d)) AS (a, b, c, d) FROM iris0" +
                ""));
        for(String s:res){
            System.out.println(s);
        }
    }

    @Test
    public void hoge() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "WITH iris AS (" +
                "  SELECT array(a, b, c, d) AS X, array(c0, c1, c2) AS Y FROM iris0)," +
                "stats AS (" +
                "  SELECT" +
                "    transpose_and_dot(Y, X) AS observed," +
                "    array_sum(X) AS feature_count," +
                "    array_avg(Y) AS class_prob" +
                "  FROM" +
                "    iris)," +
                "test AS (" +
                "  SELECT" +
                "    transpose_and_dot(class_prob, feature_count) AS expected" +
                "  FROM" +
                "    stats)" +
                "SELECT" +
                "  chi2(observed, expected) AS (chi2_val, p_val) " +
                "FROM" +
                "  test JOIN stats" +
                ""));
        for(String s:res){
            System.out.println(s);
        }
    }

    @Test
    public void poyo1() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "WITH iris AS (" +
                "  SELECT array(a, b, c, d) AS X, array(c0, c1, c2) AS Y FROM iris0)," +
                "stats AS (" +
                "  SELECT" +
                "    transpose_and_dot(Y, X) AS observed," +
                "    array_sum(X) AS feature_count," +
                "    array_avg(Y) AS class_prob" +
                "  FROM" +
                "    iris)," +
                "test AS (" +
                "  SELECT" +
                "    transpose_and_dot(class_prob, feature_count) AS expected" +
                "  FROM" +
                "    stats)," +
                "importance AS (" +
                "  SELECT" +
                "    chi2(observed, expected) AS chi2" +
                "  FROM" +
                "    test JOIN stats)" +
                "SELECT" +
                "  select_k_best(X, chi2.chi2_vals, 3)" +
                "FROM" +
                "  iris LEFT JOIN importance" +
                ""));
        for(String s:res){
            System.out.println(s);
        }
    }

    @Test
    public void aaa() throws  Exception{
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "SELECT sort_array(array_top_k_indices(array(1,3,5,1,422,42,5,1,33), 4, false))"+
                ""));
        for(String s:res){
            System.out.println(s);
        }

    }

    @Test
    public void puyo() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "WITH t AS (" +
                "  SELECT quantify(true, a, b, c, d, label) AS (a, b, c, d, label)" +
                "  FROM iris" +
                ")" +
                "SELECT column_dot_array(a, array(1.0, 2.0, 3.0))" +
                "FROM t"));
        for(String s:res){
            System.out.println(s);
        }
    }

    @Test
    public void payo() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "SELECT array_dot_array(array(1.0, 2.0), array(3.0, 4.0, 5.0))"));
        for(String s:res){
            System.out.println(s);
        }
    }

    @Test
    public void peyo0() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "SELECT transposeOne(array(array(0.0, 1.0, 2.0), array(3.0, 4.0, 5.0)))"));
        for(String s:res){
            System.out.println(s);
        }
    }

    @Test
    public void peyo1() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "SELECT transposeOne(array(array('a', 'b', 'c'), array('d', 'e', 'f')))"));
        for(String s:res){
            System.out.println(s);
        }
    }

    @Test
    public void test() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "WITH t AS (" +
                "  SELECT '1' AS a, '4' AS b" +
                "  UNION ALL" +
                "  SELECT '2' AS a, '3' AS b" +
                ")" +
                "SELECT array_avg(array(a, b)) FROM t"));
        for(String s:res){
            System.out.println(s);
        }
    }

    @Test
    public void test0() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "WITH t AS (" +
                "  SELECT array(1.0, 1.1, 1.0) AS a, array(3, 3) AS b" +
                "  UNION ALL" +
                "  SELECT array(1.0, 1.0, 2) AS a, array(4, 4) AS b" +
                ")" +
                "SELECT transpose_and_dot(a, b) FROM t"));
        for(String s:res){
            System.out.println(s);
        }
    }

    @Test
    public void test1() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "WITH t AS (" +
                "  SELECT array(array(1.1, 2.0, 3.0), array(2.0, 3.0, 4.0)) AS a, array(array(1.0, 2.0, 3.0), array(2.0, 3.0, 4.0)) AS b" +
                "  UNION ALL" +
                "  SELECT array(array(1.1, 2.0), array(2.0, 3.0), array(3.0, 4.0)) AS a, array(array(1.0, 2.0), array(2.0, 3.0), array(3.0, 4.0)) AS b" +
                ")" +
                "SELECT chi2(a, b) FROM t"));
        for(String s:res){
            System.out.println(s);
        }
    }


    @Test
    public void all() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "WITH t AS (" +
                "  SELECT quantify(true, a, b, c, d, label) AS (a, b, c, d, label)" +
                "  FROM iris" +
                "), u AS (" +
                "  SELECT array_sum_of_each_class(label, array(a, b, c, d)) AS observed" +
                "  FROM t" +
                "), v AS (" +
                "  SELECT array_sum(ob) AS features_sum" +
                "  FROM u LATERAL VIEW explode(observed) uu AS ob" +
//                "  SELECT array_sum(array(a, b, c, d)) AS features_sum" +
//                "  FROM t" +
                "), w AS (" +
                "  SELECT occurrence_prob(label) AS prob" +
                "  FROM t" +
                "), x AS (" +
                "  SELECT vector_dot_vector(prob, features_sum) AS expected" +
                "  FROM v JOIN w" +
                "), u0 AS (" +
                "  SELECT explode(transposeOne(observed)) AS observed" +
                "  FROM u" +
                "), x0 AS (" +
                "  SELECT explode(expected) AS expected" +
                "  FROM x" +
                "), u00 AS (" +
                "  SELECT row_number() over () AS id, observed" +
                "  FROM u0" +
                "), x00 AS (" +
                "  SELECT row_number() over () AS id, expected" +
                "  FROM x0" +
                "), y AS (" +
                "  SELECT chi2(expected, observed)" +
                "  FROM u00 JOIN x00 ON u00.id = x00.id" +
                ")" +
                "SELECT * from y"));
        for(String s:res){
            System.out.println(s);
        }
    }
}
