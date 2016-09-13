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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.plan.AlterTableSimpleDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.zeppelin.spark.util.HiveParse;
import org.apache.zeppelin.spark.util.TableCheckMsg;
import org.junit.Test;
import org.apache.hadoop.hive.ql.Context;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class SparkVersionTest {

  @Test
  public void testUnknownSparkVersion() {
    assertEquals(99999, SparkVersion.fromVersionString("DEV-10.10").toNumber());
  }

  @Test
  public void testUnsupportedVersion() {
    assertTrue(SparkVersion.fromVersionString("9.9.9").isUnsupportedVersion());
    assertFalse(SparkVersion.fromVersionString("1.5.9").isUnsupportedVersion());
    assertTrue(SparkVersion.fromVersionString("0.9.0").isUnsupportedVersion());
    assertTrue(SparkVersion.UNSUPPORTED_FUTURE_VERSION.isUnsupportedVersion());
    // should support spark2 version of HDP 2.5
    assertFalse(SparkVersion.fromVersionString("2.0.0.2.5.0.0-1245").isUnsupportedVersion());
  }

  @Test
  public void testSparkVersion() {
    // test equals
    assertEquals(SparkVersion.SPARK_1_2_0, SparkVersion.fromVersionString("1.2.0"));
    assertEquals(SparkVersion.SPARK_1_5_0, SparkVersion.fromVersionString("1.5.0-SNAPSHOT"));
    assertEquals(SparkVersion.SPARK_1_5_0, SparkVersion.fromVersionString("1.5.0-SNAPSHOT"));
    // test spark2 version of HDP 2.5
    assertEquals(SparkVersion.SPARK_2_0_0, SparkVersion.fromVersionString("2.0.0.2.5.0.0-1245"));

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


    // conversion
    assertEquals(10200, SparkVersion.SPARK_1_2_0.toNumber());
    assertEquals("1.2.0", SparkVersion.SPARK_1_2_0.toString());
  }


    public static void main(String[] args) throws org.apache.hadoop.hive.ql.parse.ParseException, SemanticException, IOException {
       new SparkVersionTest().sqlParseTest();
    }

    @Test
  public void sqlParseTest() throws org.apache.hadoop.hive.ql.parse.ParseException, IOException, SemanticException {
    ParseDriver parseDriver = new ParseDriver();

    System.out.println(parseDriver.parse("select  insured_premium as \"保费金额\"\n" +
      "\t\t,insured_amount as \"保费额度\"\n" +
      "\t\t,paid_amount  as \"赔付金额\"\n" +
      "\t\t,count( distinct policy_no) as \"购买次数\"\n" +
      "from ods_bx_policy\n" +
      "where status in (3,4,5,6) \n" +
      "and FROM_UNIXTIME(create_time,\"%Y-%m-%d\") = '2016-07-08' \n" +
      "group by insured_premium \n" +
      "\t\t,insured_amount \n" +
      "\t\t,paid_amount"));

    System.out.println(parseDriver.parse("select 4 as id from dua\n" +
      " union all\n" +
      "select 3 as id from dua").getChildren());
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

      ASTNode astNode2 =  parseDriver.parse("select * from ods_mb_account");
      ASTNode astNode6 =  parseDriver.parse("select a,b,c from ods_mb_account");
      ASTNode astNode3 =  parseDriver.parse("show tables");
      ASTNode astNode4 =  parseDriver.parse("insert overwrite table ss select drops from ds");
      ASTNode astNode5 =  parseDriver.parse("select count(*) from ds");
      ASTNode astNode9 =  parseDriver.parse("select task_id,works_id,a.user_id,usermobile,useremail\n" +
              "from ods_mk_works a \n" +
              "join ods_mb_account b on b.user_id=a.user_id\n" +
              "where task_id=7358857");






      ArrayList<String> testSql = new ArrayList<String>();
//      testSql.add("Select name,ip from zpc2 bieming where age > 10 and area in (select area from city)");
//      testSql.add("Select d.name,d.ip from (select * from zpc3 where age > 10 and area in (select area from city)) d");
//      testSql.add("create table zpc(id string, name string)");
//      testSql.add("insert overwrite table tmp1 PARTITION (partitionkey='2008-08-15') select * from tmp");
//      testSql.add("FROM (SELECT p.datekey datekey, p.userid userid, c.clienttype  FROM detail.usersequence_client c JOIN fact.orderpayment p ON p.orderid = c.orderid "
//              + " JOIN default.user du ON du.userid = p.userid WHERE p.datekey = 20131118 ) base  INSERT OVERWRITE TABLE `test`.`customer_kpi` SELECT base.datekey, "
//              + "  base.clienttype, count(distinct base.userid) buyer_count GROUP BY base.datekey, base.clienttype");
//      testSql.add("SELECT id, value FROM (SELECT id, value FROM p1 UNION ALL  SELECT 4 AS id, 5 AS value FROM p1 limit 1) u");
//      testSql.add("select dd from(select id+1 dd from zpc) d");
//      testSql.add("select dd+1 from(select id+1 dd from zpc) d");
//      testSql.add("truncate table zpc");
//      testSql.add("drop table zpc");
//      testSql.add("select * from tablename where unix_timestamp(cz_time) > unix_timestamp('2050-12-31 15:32:28')");
//      testSql.add("alter table old_table_name RENAME TO new_table_name");
//      testSql.add("select statis_date,time_interval,gds_cd,gds_nm,sale_cnt,discount_amt,discount_rate,price,etl_time,pay_amt from o2ostore.tdm_gds_monitor_rt where time_interval = from_unixtime(unix_timestamp(concat(regexp_replace(from_unixtime(unix_timestamp('201506181700', 'yyyyMMddHHmm')+ 84600 ,  'yyyy-MM-dd HH:mm'),'-| |:',''),'00'),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss')");
//      testSql.add("INSERT OVERWRITE TABLE u_data_new SELECT TRANSFORM (userid, movieid, rating, unixtime) USING 'python weekday_mapper.py' AS (userid, movieid, rating, weekday) FROM u_data");
//      testSql.add("SELECT a.* FROM a JOIN b ON (a.id = b.id AND a.department = b.department)");
//      testSql.add("LOAD DATA LOCAL INPATH \"/opt/data/1.txt\" OVERWRITE INTO TABLE table1");
//      testSql.add("CREATE TABLE  table1     (    column1 STRING COMMENT 'comment1',    column2 INT COMMENT 'comment2'        )");
//      testSql.add("ALTER TABLE events RENAME TO 3koobecaf");
//      testSql.add("ALTER TABLE invites ADD COLUMNS (new_col2 INT COMMENT 'a comment')");
//      testSql.add("alter table mp add partition (b='1', c='1')");
//      testSql.add("select login.uid from login day_login left outer join (select uid from regusers where dt='20130101') day_regusers on day_login.uid=day_regusers.uid where day_login.dt='20130101' and day_regusers.uid is null");
//      testSql.add("select name from (select * from zpc left outer join def) d");



//      testSql.add("select a.task_id,mode,allot,a.open_state,a.manager_name,a.state,a.category1id,category2id,category_id,a.amount,hosted_amount,task_source,d.close_ymd,refused,hosted,hosted_date from ods_mk_task a\n" +
//              "left join ods_mk_task_info b on a.task_id = b.task_id\n" +
//              "left join ods_mk_task_opis c on a.task_id = c.task_id\n" +
//              "left join ods_mk_task_extends d on a.task_id = d.task_id\n" +
//              "where createymd between '2016-05-01' and '2016-05-31'");


//      testSql.add("select a.task_id,mode,allot,a.open_state,a.manager_name,a.state,a.category1id,category2id,category_id,a.amount,hosted_amount,b.task_source,d.close_ymd,refused,hosted,hosted_date from ods_mk_task a\n" +
//              "left join ods_mk_task_info b on a.task_id = b.task_id\n" +
//              "left join ods_mk_task_opis c on a.task_id = c.task_id\n" +
//              "left join ods_mk_task_extends d on a.task_id = d.task_id\n" +
//              "where createymd between '2016-05-01' and '2016-05-31'");
//      testSql.add("select *,department_name,class_name,count(1) from ods_mk_task a \n" +
//              " left join ods_mk_bumen_category b on \n" +
//              "(b.category1id=a.category1id and b.category2id=a.category2id and b.category_id=a.category_id)\n" +
//              "left join ods_mk_bumen c on c.class_id=b.class_id\n" +
//              "left join ods_mk_task_info d on d.task_id=a.task_id\n" +
//              "where createymd between'2016-05-01' and '2016-05-31'\n" +
//              "and succeed_user_id!=12189285 and succeed_user_id!=7125354 and succeed_user_id!=11498807 and succeed_user_id!=5783562 and succeed_user_id!=14853219\n" +
//              "group by  department_name,class_name");

//      testSql.add("select user_id,usermobile,useremail from ods_mb_account where user_id in (2602269,9925147,10947386,4699370,3872435,1736742,9790298,489268,9840602,1195318,7443478,9251750,1239133,3958806,7911331,696703,6393752,8627504,1689735,2250185,12709230,653571,7784295,9079051,6001096,7946169,1685290,6944815,8946592,1453440,1118254,14873046,1314567,165490,1826779,3605365,8574204,3895753,981710,7310645,2244757,11012175,651252,4558723,655103,11388818,917218,1127637,7921513,4144615,3626781,12552771,1364623,1235523,3360711,1106008,3937355,1135501,6221373,13494294,4668401,641791,1108883,6309766,11878874,9901497,3004494,9671599)");


//        testSql.add("select task_id,s from ods_mk_task");

      testSql.add("select user_id from ods_ul_level_user union all select from ods_mb_sellerlimitsV2_user");
      HiveConf hiveConf = new HiveConf();

      //Hive 解析
//      SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(hiveConf);
//      QB qb = new QB(null,null,false);
//      ((SemanticAnalyzer) semanticAnalyzer).doPhase1(astNode9, qb,  semanticAnalyzer.initPhase1Ctx(),null);
//      QBParseInfo qbParseInfo = qb.getParseInfo();
//      qbParseInfo.getTableSpec().getPartSpec();

      HiveParse hiveParse = new HiveParse();
      for (int i = 0; i < testSql.size(); i++) {
//          System.out.println(i+1);
//          hiveParse.parse(parseDriver.parse(testSql.get(i)));
//          hiveParse.
//          System.out.println( checkSQL(parseDriver.parse(testSql.get(i))));
          System.out.println(checkTable("zhangyifei", parseDriver.parse(testSql.get(i))));
      }










        ASTNode tmp = astNode9;
//      System.out.println(tmp.dump());
//      System.out.println(checkSQL(tmp));

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

//    /**
//     * 检查是否有删除数据的操作。
//     * @param tree
//     * @return
//     */
//private boolean checkSQL(ASTNode tree) {
//    ArrayList children = tree.getChildren();
//    if(children != null) {
//        Iterator i$ = tree.getChildren().iterator();
//
//        while(i$.hasNext()) {
//            Node node = (Node)i$.next();
//            if(node instanceof ASTNode) {
//                String checkStr = node.toString();
//                if("TOK_INSERT".equals(checkStr) || "TOK_DROPTABLE".equals(checkStr) || "TOK_TRUNCATETABLE".equals(checkStr)){
//                    return false;
//                }
//                checkSQL((ASTNode) node);
//            }
//        }
//    }
//    return true;
//}

    /**
     * 检查表权限
     * @param tree
     * @return
     */
    private boolean checkSQL(ASTNode tree) {
        ArrayList children = tree.getChildren();
        if (children != null) {
            Iterator i$ = tree.getChildren().iterator();

            while (i$.hasNext()) {
                Node node = (Node) i$.next();
                if (node instanceof ASTNode) {
                    int type = ((ASTNode) node).getType();
                    boolean nb = checkSQL((ASTNode) node);
                    if (type == HiveParser.TOK_DROPTABLE || (type >= HiveParser.TOK_ALTERDATABASE_PROPERTIES
                            && type <= HiveParser.TOK_ALTERVIEW_RENAME)
                            || type == HiveParser.TOK_TRUNCATETABLE || !nb){
                        return false;
                    }
                    if (type == HiveParser.TOK_INSERT
                            && ((ASTNode) node).getChild(0).getChild(0).getChild(0).getChild(0) != null){
                        return false;
                    }
                }
            }
        }
        return true;
    }




    private TableCheckMsg checkTable(String username,ASTNode tree) {
        TableCheckMsg tableCheckMsg = new TableCheckMsg();
        tableCheckMsg.setIsSucceed(true);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "scmhadoop-1");
        TableName tableName = TableName.valueOf("zeppelin-table-denied");
        HiveParse hiveParse = new HiveParse();
        hiveParse.parse(tree);
        try {
            HTable hTable = new HTable(conf, tableName);
            String rowKey = username;
            Scan scan = new Scan();
            scan.setStartRow(username.getBytes());
            scan.setStopRow((username + "~").getBytes());
//            scan.addColumn("common".getBytes(), "amount".getBytes());

            ResultScanner rs = null;
            rs =  hTable.getScanner(scan);
//            HashMap<String,ArrayList<String>> tableCols = new HashMap<String,ArrayList<String>>();
            for (Result r : rs) {
                List<Cell> cells = r.listCells();
                if(cells != null ){

                    String table = "";
                    ArrayList<String> cols = new ArrayList<String>();

                    for (Cell cell : cells) {
                        String type = new String(CellUtil.cloneFamily(cell));
                        if(type.equals("col")){
                            cols.add(new String(CellUtil.cloneQualifier(cell)));
                        }else{
                            table = new String(CellUtil.cloneValue(cell));
                        }
//                        userTable.put(),new String(CellUtil.cloneValue(cell)));
                    }


                    if(hiveParse.getTables().contains(table)){
                        if(hiveParse.getCols().get(table+".*") != null){
                            tableCheckMsg.setIsSucceed(false);
                            tableCheckMsg.setMsg("没有访问表:" + table +"中 " + cols.toString() + "字段的权限");
                            return tableCheckMsg;
                        }

                        for (int i = 0; i < cols.size(); i++) {
                            if(hiveParse.getCols().get(table + "." + cols.get(i)) != null){
                                tableCheckMsg.setIsSucceed(false);
                                tableCheckMsg.setMsg("没有访问表:" + table +"中 " + cols.get(i) + "字段的权限");
                                return tableCheckMsg;
                            }

                            if(hiveParse.getCols().get(HiveParse.getUNKNOWN() + "." + cols.get(i)) != null){
                                tableCheckMsg.setIsSucceed(false);
                                tableCheckMsg.setMsg("字段 " + cols.get(i) + "指代不明,可能涉及敏感数据。请指定字段对应的表");
                                return tableCheckMsg;
                            }

                        }


                    }
//                    tableCols.put(table,cols);
                }
            }


            hiveParse.getTables();



        } catch (IOException e) {
            e.printStackTrace();
        }
        return tableCheckMsg;
    }


    private void checkAuth(ASTNode tree) {
        ArrayList children = tree.getChildren();
        if (children != null) {
            Iterator i$ = tree.getChildren().iterator();

            while (i$.hasNext()) {
                Node node = (Node) i$.next();
                if (node instanceof ASTNode) {
//                    checkTable( (ASTNode) node);
                } else {
                }
            }
        } else {
            if (tree.parent.toString().equals("TOK_TABNAME"))
                System.out.println(tree);
        }
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

//    /**
//     * 获取涉及的相关表。
//     * @param tree
//     */
//    private void dump(ASTNode tree) {
//        ArrayList children = tree.getChildren();
//        if(children != null) {
//            Iterator i$ = tree.getChildren().iterator();
//
//            while(i$.hasNext()) {
//                Node node = (Node)i$.next();
//                if(node instanceof ASTNode) {
//                    dump((ASTNode) node);
//                } else {
//                }
//            }
//        }else{
//            if(tree.parent.toString().equals("TOK_TABNAME"))
//                System.out.println(tree);
//        }
//    }
//    private boolean findCol(String col,){
//
//    }
  @Test
  public void setUp() throws Exception {
    String s = "2 2 2 2 2\t";
    String ss = s.replaceAll("\t",",");
  }
}
