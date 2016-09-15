package hivemall.tools.array;

import hivemall.TestOfTest;
import hivemall.systemtest.model.HQ;
import hivemall.systemtest.runner.HiveSystemTestRunner;
import hivemall.systemtest.runner.SystemTestCommonInfo;
import hivemall.systemtest.runner.SystemTestTeam;
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
            initBy(HQ.fromStatements("" +
                    "CREATE TEMPORARY FUNCTION quantify as 'hivemall.ftvec.conv.QuantifyColumnsUDTF';" +
                    "CREATE TEMPORARY FUNCTION array_sum_of_each_class as 'hivemall.tools.array.ArraySumOfEachClassUDAF';" +
                    "CREATE TEMPORARY FUNCTION occurrence_prob as 'hivemall.tools.count.OccurrenceProbUDAF';" +
                    ""));
        }
    };

    @Rule
    public SystemTestTeam team = new SystemTestTeam(hRunner);

    @Test
    public void piyo() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "WITH t AS (" +
                "  SELECT quantify(true, a, b, c, d, label) AS (a, b, c, d, label)" +
                "  FROM iris" +
                ")" +
                "SELECT array_sum_of_each_class(label, array(a, b, c, d))" +
                "FROM t"));
        for(String s:res){
            System.out.println(s);
        }
    }

    @Test
    public void poyo() throws Exception {
        List<String> res = hRunner.exec(HQ.fromStatement("" +
                "WITH t AS (" +
                "  SELECT quantify(true, a, b, c, d, label) AS (a, b, c, d, label)" +
                "  FROM iris" +
                ")" +
                "SELECT occurrence_prob(label)" +
                "FROM t"));
        for(String s:res){
            System.out.println(s);
        }
    }
}
