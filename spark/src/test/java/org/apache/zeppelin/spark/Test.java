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
    String st = "select t1.uniqid, enter_url, jump_fir_domainname, jump_sec_domainname, jump_thi_domainname\n" +
      "from\n" +
      "(select uniqid, current_url as enter_url\n" +
      "from fact_galog_behavior\n" +
      "where current_domainname='task.zbj.com' and currentparam='/' and time='2016-09-06' and visit_num=1) t1\n" +
      "left join\n" +
      "(select uniqid, current_domainname as jump_fir_domainname\n" +
      "from fact_galog_behavior\n" +
      "where time='2016-09-06' and visit_num=2) t2 on t1.uniqid=t2.uniqid\n" +
      "left join\n" +
      "(select uniqid, current_domainname as jump_sec_domainname\n" +
      "from fact_galog_behavior\n" +
      "where time='2016-09-06' and visit_num=3) t3 on t1.uniqid=t3.uniqid\n" +
      "left join\n" +
      "(select uniqid, current_domainname as jump_thi_domainname\n" +
      "from fact_galog_behavior\n" +
      "where time='2016-09-06' and visit_num=4) t4 on t1.uniqid=t4.uniqid";
    ParseDriver parseDriver = new ParseDriver();
    ASTNode astNode = null;
    try {
      astNode = parseDriver.parse(st);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
}
