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
package hivemall;

import hivemall.systemtest.model.HQ;
import hivemall.systemtest.model.RawHQ;
import hivemall.systemtest.model.lazy.LazyMatchingResource;
import hivemall.systemtest.runner.HiveSystemTestRunner;
import hivemall.systemtest.runner.SystemTestCommonInfo;
import hivemall.systemtest.runner.SystemTestTeam;
import hivemall.systemtest.runner.TDSystemTestRunner;
import hivemall.systemtest.utils.IO;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

public class TestOfTest {
    private static SystemTestCommonInfo ci = new SystemTestCommonInfo(TestOfTest.class);

    @ClassRule
    public static HiveSystemTestRunner hRunner = new HiveSystemTestRunner(ci) {
        {
            initBy(HQ.uploadByResourcePathAsNewTable("color", ci.initDir + "color.tsv",
                new LinkedHashMap<String, String>() {
                    {
                        put("name", "string");
                        put("red", "int");
                        put("green", "int");
                        put("blue", "int");
                    }
                }));
            initBy(HQ.fromResourcePath(ci.initDir + "init"));
        }
    };
    /*
        @ClassRule
        public static TDSystemTestRunner tRunner = new TDSystemTestRunner(ci) {
            {
                initBy(HQ.uploadByResourcePathAsNewTable("color", ci.initDir + "color.tsv",
                        new LinkedHashMap<String, String>() {
                            {
                                put("name", "string");
                                put("red", "int");
                                put("green", "int");
                                put("blue", "int");
                            }
                        }));
            }
        };
    */
    //    @ClassRule
    public static TDSystemTestRunner tRunner0 = new TDSystemTestRunner(ci);

    @Rule
    public ExpectedException predictor = ExpectedException.none();

    @Rule
    public SystemTestTeam team = new SystemTestTeam(hRunner);


    @Test
    public void arrayAndMap() throws Exception {
        /*
        tRunner.exec(HQ.insert("color", Arrays.asList("name", "red", "green", "blue"),
                Arrays.asList(
                        new Object[]{"darkcyan", 0, 139, 139},
                        new Object[]{"black", 0, 0, 0}
                )));
        for(String r: tRunner.exec(HQ.fromStatement("SELECT * FROM color"))){
            System.out.println(r);
        }*/

        tRunner0.exec(HQ.createTable("color0", new LinkedHashMap<String, String>() {
            {
                put("name", "string");
                //            put("rgb", "map<string, array<int>>");
                //            put("rgb", "map<string, int>");
                put("rgb", "array<int>");
            }
        }));

        tRunner0.exec(HQ.insert(
            "color0",
            Arrays.asList("name", "rgb"),
            Arrays.asList(
                //                hRunner.exec(HQ.insert("color0", Arrays.asList("name"), Arrays.asList(
                new Object[] {"black", Arrays.asList(0, 0, 0)},
                new Object[] {"white", Arrays.asList(255, 255, 255)}
            //                new Object[]{"ll"},
            //                new Object[]{"bb"}
            )));

        //        hRunner.exec(HQ.fromStatement(
        //                "INSERT INTO TABLE color0 VALUES ('black2', array(0, 0, 0))"
        //        ));
        /*
                tRunner0.exec(HQ.insert("color0", Arrays.asList("name", "rgb"), Arrays.asList(
                        new Object[]{"black", new HashMap<String, List<Integer>>(){{
        //                    put("red", 0);
                            put("red", Arrays.asList(0, 0, 0));
        //                    put("green", 0);
                            put("green", Arrays.asList(0, 0, 0));
        //                    put("blue", 0);
                            put("blue", Arrays.asList(0, 0, 0));
                        }}},
                        new Object[]{"white", new HashMap<String, List<Integer>>(){{
        //                    put("red", 255);
                            put("red", Arrays.asList(0, 0, 0));
        //                    put("green", 255);
                            put("green", Arrays.asList(0, 0, 0));
        //                    put("blue", 255);
                            put("blue", Arrays.asList(0, 0, 0));
                        }}}//,
        //                new Object[]{"red", new HashMap<String, Integer[]>(){{
        //                    put("red", new Integer[]{2, 5, 5});
        //                    put("green", new Integer[]{0, 0, 0});
        //                    put("blue", new Integer[]{0, 0, 0});
        //                }}}
                )));
        */
        for (String s : tRunner0.exec(HQ.fromStatement("SELECT * FROM color0"))) {
            System.out.println(s);
        }
    }

    @Test
    public void across() throws Exception {
        String tableName = "users";
        team.initBy(HQ.createTable(tableName, new LinkedHashMap<String, String>() {
            {
                put("name", "string");
                put("age", "int");
                put("favorite_color", "string");
            }
        }));
        team.initBy(HQ.insert(tableName, Arrays.asList("name", "age", "favorite_color"),
            Arrays.asList(new Object[] {"Karen", 16, "orange"}, new Object[] {"Alice", 17, "pink"})));
        team.set(
            HQ.fromStatement("SELECT CONCAT('rgb(', red, ',', green, ',', blue, ')') FROM "
                    + tableName + " u LEFT JOIN color c on u.favorite_color = c.name"),
            "rgb(255,165,0)\trgb(255,192,203)");
        team.run();
    }

    @Test
    public void order0() throws Exception {
        team.set(HQ.fromStatement("SELECT name FROM color WHERE blue = 255 ORDER BY name ASC"),
            "azure\tblue\tmagenta", true);
        team.run();
    }

    @Test
    public void order1() throws Exception {
        predictor.expect(Throwable.class);
        team.set(HQ.fromStatement("SELECT name FROM color WHERE blue = 255 ORDER BY name DESC"),
            "azure\tblue\tmagenta", true);
        team.run();
    }

    @Test
    public void uploadFile0() throws Exception {
        String tableName = "color0";
        team.initBy(HQ.createTable(tableName, new LinkedHashMap<String, String>() {
            {
                put("name", "string");
                put("red", "int");
                put("green", "int");
                put("blue", "int");
            }
        }));
        team.initBy(HQ.uploadByResourcePathToExisting(tableName, ci.initDir + "color.tsv"));
        team.set(HQ.fromStatement("SELECT COUNT(1) FROM " + tableName), "11");
        team.run();
    }

    @Test
    public void uploadFile1() throws Exception {
        String tableName = "color0";
        team.initBy(HQ.createTable(tableName, new LinkedHashMap<String, String>() {
            {
                put("name", "string");
                put("red", "int");
                put("green", "int");
                put("blue", "int");
            }
        }));
        team.initBy(HQ.uploadByResourcePathToExisting(tableName, ci.initDir + "color.csv"));
        team.set(HQ.fromStatement("SELECT COUNT(1) FROM " + tableName), "11");
        team.run();
    }

    @Test
    public void fromStatement() throws Exception {
        team.set(HQ.fromStatement("SELECT hivemall_version();"), "0.4.2-rc.2");
        team.run();
    }

    @Test
    public void fromStatements() throws Exception {
        team.initBy(HQ.fromStatements("SELECT hivemall_version();SELECT hivemall_version();"));
        team.set(HQ.fromStatements("SELECT hivemall_version();SELECT hivemall_version();"),
            Arrays.asList("0.4.2-rc.2", "0.4.2-rc.2"));
        team.run();
    }

    @Test
    public void fromFileName() throws Exception {
        team.set(HQ.autoMatchingByFileName("test0"), ci);
        team.run();
    }

    @Test
    public void fromFilePath() throws Exception {
        team.set(HQ.fromResourcePath("hivemall/TestOfTest/case/test0"),
            Arrays.asList("0.4.2-rc.2", "0.4.2-rc.2", "0.4.2-rc.2", "0.4.2-rc.2"));
        team.run();
    }

    @Test
    public void fromFilePathAndGetAnswers() throws Exception {
        team.set(HQ.fromResourcePath(ci.caseDir + "test0"),
            Arrays.asList(IO.getFromResourcePath(ci.answerDir + "test0").split(IO.QD)));
        team.run();
    }

    @Test
    public void rawAPI0() throws Exception {
        for (RawHQ q : HQ.fromResourcePath(ci.caseDir + "test0")) {
            hRunner.exec(q);
        }
    }

    @Test
    public void rawAPI1() throws Exception {
        hRunner.matching(HQ.fromStatement("SELECT hivemall_version()"), "0.4.2-rc.2");
    }

    @Test
    public void rawAPI2() throws Exception {
        predictor.expect(Throwable.class);
        hRunner.matching(HQ.fromStatement("SELECT hivemall_version()"), "invalid");
    }

    @Test
    public void rawAPI3() throws Exception {
        LazyMatchingResource lr = HQ.autoMatchingByFileName("test0");
        List<RawHQ> rhqs = lr.toStrict(ci.caseDir);
        String[] answers = lr.getAnswers(ci.answerDir);

        Assert.assertEquals(rhqs.size(), answers.length);

        for (int i = 0; i < answers.length; i++) {
            hRunner.matching(rhqs.get(i), answers[i]);
        }
    }
}
