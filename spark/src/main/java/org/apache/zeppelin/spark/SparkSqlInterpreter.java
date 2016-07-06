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

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.LazyOpenInterpreter;
import org.apache.zeppelin.interpreter.WrappedInterpreter;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.spark.util.HiveParse;
import org.apache.zeppelin.spark.util.TableCheckMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark SQL interpreter for Zeppelin.
 */
public class SparkSqlInterpreter extends Interpreter {
  public static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
  Logger logger = LoggerFactory.getLogger(SparkSqlInterpreter.class);
  AtomicInteger num = new AtomicInteger(0);

  private String getJobGroup(InterpreterContext context) {
    return "zeppelin-" + context.getParagraphId();
  }

  private int maxResult;
  private int maxExport;

  public SparkSqlInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    this.maxResult = Integer.parseInt(getProperty("zeppelin.spark.maxResult"));
    this.maxExport = Integer.parseInt(getProperty("zeppelin.spark.maxExport"));
  }

  private SparkInterpreter getSparkInterpreter() {
    LazyOpenInterpreter lazy = null;
    SparkInterpreter spark = null;
    Interpreter p = getInterpreterInTheSameSessionByClassName(SparkInterpreter.class.getName());

    while (p instanceof WrappedInterpreter) {
      if (p instanceof LazyOpenInterpreter) {
        lazy = (LazyOpenInterpreter) p;
      }
      p = ((WrappedInterpreter) p).getInnerInterpreter();
    }
    spark = (SparkInterpreter) p;

    if (lazy != null) {
      lazy.open();
    }
    return spark;
  }

  public boolean concurrentSQL() {
    return Boolean.parseBoolean(getProperty("zeppelin.spark.concurrentSQL"));
  }

  @Override
  public void close() {
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {

    String currentUser = context.getAuthenticationInfo().getUser();
//    context.get
    this.insertSQLOperator(currentUser, st);
//    ParseDriver.HiveLexerX

    SQLContext sqlc = null;
    SparkInterpreter sparkInterpreter = getSparkInterpreter();

    if (sparkInterpreter.getSparkVersion().isUnsupportedVersion()) {
      return new InterpreterResult(Code.ERROR, "Spark "
          + sparkInterpreter.getSparkVersion().toString() + " is not supported");
    }
    ParseDriver parseDriver = new ParseDriver();
    ASTNode astNode = null;
    try {
      astNode = parseDriver.parse(st);
    } catch (ParseException e) {
      e.printStackTrace();
      return new InterpreterResult(Code.ERROR, "SQL语义解析错误,请检查SQL");
    }
    if (!checkSQL(astNode)) {
      return new InterpreterResult(Code.ERROR, "你没有删改数据的权限,请检查SQL。该操作已被记录");
    }

    TableCheckMsg tableGlobelCheckMsg =
      checkTable(getProperty("zeppelin.SQL.auth.denied.globeluser"), astNode);
    if (!tableGlobelCheckMsg.isSucceed()) {
      return new InterpreterResult(Code.ERROR, tableGlobelCheckMsg.getMsg());
    }

    TableCheckMsg tableCheckMsg = checkTable(currentUser, astNode);
    if (!tableCheckMsg.isSucceed()) {
      return new InterpreterResult(Code.ERROR, tableCheckMsg.getMsg());
    }

    sqlc = getSparkInterpreter().getSQLContext();
    SparkContext sc = sqlc.sparkContext();
    if (concurrentSQL()) {
      sc.setLocalProperty("spark.scheduler.pool", "fair");
    } else {
      sc.setLocalProperty("spark.scheduler.pool", null);
    }

    sc.setJobGroup(getJobGroup(context), "Zeppelin", false);
    Object rdd = null;
    try {
      // method signature of sqlc.sql() is changed
      // from  def sql(sqlText: String): SchemaRDD (1.2 and prior)
      // to    def sql(sqlText: String): DataFrame (1.3 and later).
      // Therefore need to use reflection to keep binary compatibility for all spark versions.
      Method sqlMethod = sqlc.getClass().getMethod("sql", String.class);
      rdd = sqlMethod.invoke(sqlc, st);
    } catch (InvocationTargetException ite) {
      if (Boolean.parseBoolean(getProperty("zeppelin.spark.sql.stacktrace"))) {
        throw new InterpreterException(ite);
      }
      logger.error("Invocation target exception", ite);
      String msg = ite.getTargetException().getMessage()
          + "\nset zeppelin.spark.sql.stacktrace = true to see full stacktrace";
      return new InterpreterResult(Code.ERROR, msg);
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException
        | IllegalArgumentException e) {
      throw new InterpreterException(e);
    }

    String msg = ZeppelinContext.showDF(sc, context, rdd, maxResult);
    int row = msg.split("\n").length;
    if (row >=  maxResult) {
      String export = ZeppelinContext.showDF(sc, context, rdd, maxExport);
      exportBigResult(context.getNoteId() + context.getParagraphId(), export, currentUser);
    }
    sc.clearJobGroup();
    return new InterpreterResult(Code.SUCCESS, msg);
  }

  @Override
  public void cancel(InterpreterContext context) {
    SQLContext sqlc = getSparkInterpreter().getSQLContext();
    SparkContext sc = sqlc.sparkContext();

    sc.cancelJobGroup(getJobGroup(context));
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }


  @Override
  public int getProgress(InterpreterContext context) {
    SparkInterpreter sparkInterpreter = getSparkInterpreter();
    return sparkInterpreter.getProgress(context);
  }

  @Override
  public Scheduler getScheduler() {
    if (concurrentSQL()) {
      int maxConcurrency = 10;
      return SchedulerFactory.singleton().createOrGetParallelScheduler(
          SparkSqlInterpreter.class.getName() + this.hashCode(), maxConcurrency);
    } else {
      // getSparkInterpreter() calls open() inside.
      // That means if SparkInterpreter is not opened, it'll wait until SparkInterpreter open.
      // In this moment UI displays 'READY' or 'FINISHED' instead of 'PENDING' or 'RUNNING'.
      // It's because of scheduler is not created yet, and scheduler is created by this function.
      // Therefore, we can still use getSparkInterpreter() here, but it's better and safe
      // to getSparkInterpreter without opening it.

      Interpreter intp =
          getInterpreterInTheSameSessionByClassName(SparkInterpreter.class.getName());
      if (intp != null) {
        return intp.getScheduler();
      } else {
        return null;
      }
    }
  }

  /**
   * 检查表权限
   *
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
          if (type == HiveParser.TOK_DROPTABLE || type == HiveParser.TOK_ALTERTABLE
              || type == HiveParser.TOK_TRUNCATETABLE || !nb) {
            return false;
          }
          if (type == HiveParser.TOK_INSERT
              && ((ASTNode) node).getChild(0).getChild(0).getChild(0).getChild(0) != null) {
            return false;
          }
        }
      }
    }
    return true;
  }


  /**
   * 检查涉及的表权限。
   *
   * @param tree
   */
  private void checkTable(ASTNode tree) {
    ArrayList children = tree.getChildren();
    if (children != null) {
      Iterator i$ = tree.getChildren().iterator();

      while (i$.hasNext()) {
        Node node = (Node) i$.next();
        if (node instanceof ASTNode) {
          checkTable((ASTNode) node);
        } else {
        }
      }
    } else {
      if (tree.parent.toString().equals("TOK_TABNAME"))
        System.out.println(tree);
    }
  }

  private void insertSQLOperator(String user, String sql) {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", getProperty("hbase.zookeeper.quorum"));
    TableName tableName = TableName.valueOf(getProperty("zeppelin.SQL.cmd.tablename"));
    logger.info(getProperty("hbase.zookeeper.quorum"));
    logger.info(getProperty("zeppelin.SQL.cmd.tablename"));


    try {
      HTable hTable = new HTable(conf, tableName);
      Long cur = System.currentTimeMillis();
      String time = String.valueOf(cur);
      String rowKey = time + user;

      Put put = new Put(rowKey.getBytes());
      put.addImmutable("common".getBytes(), "time".getBytes(),
          SDF.format(new Date(cur)).getBytes());
      put.addImmutable("common".getBytes(), "sql".getBytes(), sql.getBytes());
      put.addImmutable("common".getBytes(), "user".getBytes(), user.getBytes());
      hTable.put(put);
      hTable.close();

    } catch (IOException e) {
      e.printStackTrace();
      logger.error(e.getMessage());
    }
  }

  /**
   * 检查表权限
   *
   * @param username
   * @param tree
   * @return
   */
  private TableCheckMsg checkTable(String username, ASTNode tree) {
    TableCheckMsg tableCheckMsg = new TableCheckMsg();
    tableCheckMsg.setIsSucceed(true);
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", getProperty("hbase.zookeeper.quorum"));
    TableName tableName = TableName.valueOf(getProperty("zeppelin.SQL.auth.denied.tablename"));
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
      rs = hTable.getScanner(scan);
      for (Result r : rs) {
        List<Cell> cells = r.listCells();
        if (cells != null) {
          String table = "";
          ArrayList<String> cols = new ArrayList<String>();

          for (Cell cell : cells) {
            String type = new String(CellUtil.cloneFamily(cell));
            if (type.equals("col")) {
              cols.add(new String(CellUtil.cloneQualifier(cell)));
            } else {
              table = new String(CellUtil.cloneValue(cell));
            }
          }
          if (hiveParse.getTables().contains(table)) {
            if (hiveParse.getCols().get(HiveParse.getUNKNOWN() + ".*") != null) {
              tableCheckMsg.setIsSucceed(false);
              tableCheckMsg.setMsg("没有访问表:" + table + "中 " + cols.toString() + "字段的权限");
              hTable.close();
              return tableCheckMsg;
            }

            if (hiveParse.getCols().get(table + ".*") != null) {
              tableCheckMsg.setIsSucceed(false);
              tableCheckMsg.setMsg("没有访问表:" + table + "中 " + cols.toString() + "字段的权限");
              hTable.close();
              return tableCheckMsg;
            }

            for (int i = 0; i < cols.size(); i++) {
              if (hiveParse.getCols().get(table + "." + cols.get(i)) != null) {
                tableCheckMsg.setIsSucceed(false);
                tableCheckMsg.setMsg("没有访问表:" + table + "中 " + cols.get(i) + "字段的权限");
                hTable.close();
                return tableCheckMsg;
              }

              if (hiveParse.getCols().get(HiveParse.getUNKNOWN() + "." + cols.get(i)) != null) {
                tableCheckMsg.setIsSucceed(false);
                tableCheckMsg.setMsg("字段 " + cols.get(i) + "是模糊的,可能涉及"
                  + table + "表中的敏感数据。请指定字段对应的表");
                hTable.close();
                return tableCheckMsg;
              }
            }
          }
        }
      }
      hTable.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return tableCheckMsg;
  }

  private void exportBigResult(String key , String msg, String user) {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", getProperty("hbase.zookeeper.quorum"));
    TableName tableName = TableName.valueOf(getProperty("zeppelin.SQL.cache.tablename"));

    try {
      HTable hTable = new HTable(conf, tableName);
      Long cur = System.currentTimeMillis();
      String rowKey = key;
      String path = "/zeppelin/" + UUID.randomUUID().toString();

      Put put = new Put(rowKey.getBytes());
      put.addImmutable("common".getBytes(), "path".getBytes(), path.getBytes());
      put.addImmutable("common".getBytes(), "user".getBytes(), user.getBytes());
      put.addImmutable("common".getBytes(), "time".getBytes(),
        SDF.format(new Date(cur)).getBytes());
      hTable.put(put);
      hTable.close();

      Configuration hdfsconf = new Configuration();
      hdfsconf.addResource(new org.apache.hadoop.fs.Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
      hdfsconf.addResource(new org.apache.hadoop.fs.Path("/home/hadoop/hadoop/conf/core-site.xml"));
      hdfsconf.addResource(
        new org.apache.hadoop.fs.Path("/home/hadoop/hadoop/conf/mapred-site.xml"));
      FileSystem fileSystem = FileSystem.get(hdfsconf);
      FSDataOutputStream out = fileSystem.create(new Path(path));
      InputStream inputStream = new ByteArrayInputStream(msg.getBytes("UTF-8"));

      byte[] buffer = new byte[8096 * 100];
      int c = 0;
      while (( c = inputStream.read(buffer, 0, buffer.length)) != -1) {
        out.write(buffer, 0, c);
        out.flush();
      }
      out.close();
      fileSystem.close();
    } catch (IOException e) {
      e.printStackTrace();
      logger.error(e.getMessage());
    }
  }

  private void writeData(FSDataOutputStream out, byte[] buffer, int length) throws IOException {

    int totalByteWritten = 0;
    int remainToWrite = length;

    while (remainToWrite > 0) {
      int toWriteThisRound = remainToWrite > buffer.length ? buffer.length
        : remainToWrite;
      out.write(buffer, 0, toWriteThisRound);
      totalByteWritten += toWriteThisRound;
      remainToWrite -= toWriteThisRound;
    }
    if (totalByteWritten != length) {
      throw new IOException("WriteData: failure in write. Attempt to write "
        + length + " ; written=" + totalByteWritten);
    }
  }


  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor) {
    return null;
  }
}
