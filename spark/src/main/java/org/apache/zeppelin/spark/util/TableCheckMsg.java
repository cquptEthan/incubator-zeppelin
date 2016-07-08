package org.apache.zeppelin.spark.util;

/**
 * @author Ethan
 * @comment
 *  16/6/23
 */
public class TableCheckMsg {
  private boolean isSucceed;
  private String msg;
  private String table;
  private String col;

  public boolean isSucceed() {
    return isSucceed;
  }

  public void setIsSucceed(boolean isSucceed) {
    this.isSucceed = isSucceed;
  }

  public String getMsg() {
    return msg;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getCol() {
    return col;
  }

  public void setCol(String col) {
    this.col = col;
  }
}
