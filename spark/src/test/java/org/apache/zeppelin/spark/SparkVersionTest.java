/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.spark;

import static org.junit.Assert.*;

import antlr.RecognitionException;
import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.plan.AlterTableSimpleDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Test;
import org.apache.hadoop.hive.ql.Context;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SparkVersionTest {

  @Test
  public void testUnknownSparkVersion() {
    assertEquals(999, SparkVersion.fromVersionString("DEV-10.10").toNumber());
  }

  @Test
  public void testUnsupportedVersion() {
    assertTrue(SparkVersion.fromVersionString("9.9.9").isUnsupportedVersion());
    assertFalse(SparkVersion.fromVersionString("1.5.9").isUnsupportedVersion());
    assertTrue(SparkVersion.fromVersionString("0.9.0").isUnsupportedVersion());
    assertTrue(SparkVersion.UNSUPPORTED_FUTURE_VERSION.isUnsupportedVersion());
  }

  @Test
  public void testSparkVersion() {
    // test equals
    assertEquals(SparkVersion.SPARK_1_2_0, SparkVersion.fromVersionString("1.2.0"));
    assertEquals(SparkVersion.SPARK_1_5_0, SparkVersion.fromVersionString("1.5.0-SNAPSHOT"));

    // test newer than
    assertFalse(SparkVersion.SPARK_1_2_0.newerThan(SparkVersion.SPARK_1_2_0));
    assertFalse(SparkVersion.SPARK_1_2_0.newerThan(SparkVersion.SPARK_1_3_0));
    assertTrue(SparkVersion.SPARK_1_2_0.newerThan(SparkVersion.SPARK_1_1_0));

    assertTrue(SparkVersion.SPARK_1_2_0.newerThanEquals(SparkVersion.SPARK_1_2_0));
    assertFalse(SparkVersion.SPARK_1_2_0.newerThanEquals(SparkVersion.SPARK_1_3_0));
    assertTrue(SparkVersion.SPARK_1_2_0.newerThanEquals(SparkVersion.SPARK_1_1_0));

    // test older than
    assertFalse(SparkVersion.SPARK_1_2_0.olderThan(SparkVersion.SPARK_1_2_0));
    assertFalse(SparkVersion.SPARK_1_2_0.olderThan(SparkVersion.SPARK_1_1_0));
    assertTrue(SparkVersion.SPARK_1_2_0.olderThan(SparkVersion.SPARK_1_3_0));

    assertTrue(SparkVersion.SPARK_1_2_0.olderThanEquals(SparkVersion.SPARK_1_2_0));
    assertFalse(SparkVersion.SPARK_1_2_0.olderThanEquals(SparkVersion.SPARK_1_1_0));
    assertTrue(SparkVersion.SPARK_1_2_0.olderThanEquals(SparkVersion.SPARK_1_3_0));

    assertEquals(120, SparkVersion.SPARK_1_2_0.toNumber());
    assertEquals("1.2.0", SparkVersion.SPARK_1_2_0.toString());
  }

  @Test
  public void sqlParseTest() throws org.apache.hadoop.hive.ql.parse.ParseException, IOException, SemanticException {
    ParseDriver parseDriver = new ParseDriver();
    System.out.println(parseDriver.parse("select * from s").getChildren());
    System.out.println(parseDriver.parse("drop table s").getChildren());
    System.out.println(parseDriver.parse("from tem_seo_info t\n" +
            "insert overwrite table tem_seo_all select 'all_seo' as content ,count(distinct t.uniqid),'${dtStr}'   where t.qdid='1'\n" +
            "insert overwrite table tem_seo_baidu select 'all_baidu' as content ,count(distinct t.uniqid),'${dtStr}'   where t.qdlyid='2'\n" +
            "insert overwrite table tem_seo_haosou select 'all_haosou' as content ,count(distinct t.uniqid),'${dtStr}'   where t.qdlyid='1'\n" +
            "insert overwrite table tem_seo_sogou select 'all_sogou' as content ,count(distinct t.uniqid),'${dtStr}'   where t.qdlyid='3'\n" +
            "insert overwrite table tem_seo_sm select 'all_shenma' as content ,count(distinct t.uniqid),'${dtStr}'   where t.qdlyid='9'\n" +
            "insert overwrite table tem_baidu_domainname select concat(t.domainname,'_','baidu'),count(distinct t.uniqid),'${dtStr}' where t.qdlyid='2' group by t.domainname\n" +
            "insert overwrite table tem_haosou_domainname select concat(t.domainname,'_','haosou'),count(distinct t.uniqid),'${dtStr}' where t.qdlyid='1' group by t.domainname\n" +
            "insert overwrite table tem_sogou_domainname select concat(t.domainname,'_','sogou'),count(distinct t.uniqid),'${dtStr}' where t.qdlyid='3' group by t.domainname\n" +
            "insert overwrite table tem_shenma_domainname select concat(t.domainname,'_','shenma'),count(distinct t.uniqid),'${dtStr}' where t.qdlyid='9' group by t.domainname\n" +
            "insert overwrite table tem_seo_domainname select concat(t.domainname,'_','seo'),count(distinct t.uniqid),'${dtStr}' where t.qdid='1' group by t.domainname").getChildren());
    ASTNode astNode = (ASTNode) parseDriver.parse("from tem_seo_info t\n" +
            "insert overwrite table tem_seo_all select 'all_seo' as content ,count(distinct t.uniqid),'${dtStr}'   where t.qdid='1'\n" +
            "insert overwrite table tem_seo_baidu select 'all_baidu' as content ,count(distinct t.uniqid),'${dtStr}'   where t.qdlyid='2'\n" +
            "insert overwrite table tem_seo_haosou select 'all_haosou' as content ,count(distinct t.uniqid),'${dtStr}'   where t.qdlyid='1'\n" +
            "insert overwrite table tem_seo_sogou select 'all_sogou' as content ,count(distinct t.uniqid),'${dtStr}'   where t.qdlyid='3'\n" +
            "insert overwrite table tem_seo_sm select 'all_shenma' as content ,count(distinct t.uniqid),'${dtStr}'   where t.qdlyid='9'\n" +
            "insert overwrite table tem_baidu_domainname select concat(t.domainname,'_','baidu'),count(distinct t.uniqid),'${dtStr}' where t.qdlyid='2' group by t.domainname\n" +
            "insert overwrite table tem_haosou_domainname select concat(t.domainname,'_','haosou'),count(distinct t.uniqid),'${dtStr}' where t.qdlyid='1' group by t.domainname\n" +
            "insert overwrite table tem_sogou_domainname select concat(t.domainname,'_','sogou'),count(distinct t.uniqid),'${dtStr}' where t.qdlyid='3' group by t.domainname\n" +
            "insert overwrite table tem_shenma_domainname select concat(t.domainname,'_','shenma'),count(distinct t.uniqid),'${dtStr}' where t.qdlyid='9' group by t.domainname\n" +
            "insert overwrite table tem_seo_domainname select concat(t.domainname,'_','seo'),count(distinct t.uniqid),'${dtStr}' where t.qdid='1' group by t.domainname").getChild(0);

      ASTNode astNode2 = (ASTNode) parseDriver.parse("truncate table drop");
        ASTNode tmp = astNode2;
      checkSQL(tmp);
//    System.out.println(s);

//    while (tmp != null){
//      tmp.getChildren().get(0).getName();
//
//    }
//    List<org.apache.hadoop.hive.ql.exec.Task<? extends Serializable>> roots = baseSemanticAnalyzer.getRootTasks();
//    return ((DDLWork)roots.get(0).getWork()).getAlterTblSimpleDesc();
//    System.out.println(astNode.getChildren());

  }
//    public String dump(ASTNode tree) {
//        this.dump(tree);
//    }

    /**
     * 检查是否有删除数据的操作。
     * @param tree
     * @return
     */
private boolean checkSQL(ASTNode tree) {
    ArrayList children = tree.getChildren();
    if(children != null) {
        Iterator i$ = tree.getChildren().iterator();

        while(i$.hasNext()) {
            Node node = (Node)i$.next();
            if(node instanceof ASTNode) {
                String checkStr = node.toString();
                if("TOK_INSERT".equals(checkStr) || "TOK_DROPTABLE".equals(checkStr) || "TOK_TRUNCATETABLE".equals(checkStr)){
                    return false;
                }
                checkSQL((ASTNode) node);
            }
        }
    }
    return true;
}

    /**
     * 获取涉及的相关表。
     * @param tree
     */
    private void dump(ASTNode tree) {
        ArrayList children = tree.getChildren();
        if(children != null) {
            Iterator i$ = tree.getChildren().iterator();

            while(i$.hasNext()) {
                Node node = (Node)i$.next();
                if(node instanceof ASTNode) {
                    dump((ASTNode) node);
                } else {
                }
            }
        }else{
            if(tree.parent.toString().equals("TOK_TABNAME"))
                System.out.println(tree);
        }
    }
}
