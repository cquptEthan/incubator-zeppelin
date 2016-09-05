package org.apache.zeppelin.spark;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

/**
 * @author Ethan
 * @comment
 * @date 16/8/30
 */
public class Test {
  @org.junit.Test
  public void s() {
    String st = "select count(distinct a.user_id) from (select distinct user_id from ods_ad_new_record where create_date between  '2016-07-01' and '2016-07-31' union all select user_id from ods_mb_sellerlimitsV2_user where membership_type>0 and create_ymd between '2016-07-01' and '2016-07-31')a \n" +
      "where a.user_id not in (select user_id from (SELECT t.user_id,SUM(case when dateymd<'2016-07-01'  then in_amount end) AS preshouru\n" +
      "FROM ods_pa_platform_bill t\n" +
      "WHERE stype_id IN (6,10,24,26) AND in_amount >0 AND t.user_id <>0\n" +
      "GROUP BY t.user_id)tt where preshouru>0)";
    ParseDriver parseDriver = new ParseDriver();
    ASTNode astNode = null;
    try {
      astNode = parseDriver.parse(st);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
}
