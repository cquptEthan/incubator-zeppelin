package org.apache.zeppelin.rest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
/**
 * @author Ethan
 * @comment
 * @date 16/7/1
 */

import org.apache.hadoop.io.IOUtils;
import org.apache.zeppelin.interpreter.InterpreterResult;
  import org.apache.zeppelin.notebook.Note;
  import org.apache.zeppelin.notebook.Notebook;
  import org.apache.zeppelin.notebook.Paragraph;
  import org.apache.zeppelin.server.JsonResponse;
  import org.slf4j.Logger;
  import org.slf4j.LoggerFactory;

  import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
  import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Rest api endpoint for the noteBook.
 */
@Path("/export")
@Produces("application/text")
public class ExporterRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(ExporterRestApi.class);
  private Notebook notebook;

  public ExporterRestApi() {
  }

  public ExporterRestApi(Notebook notebook) {
    this.notebook = notebook;
  }

  /**
   * Run paragraph job and return the results as a CSV file
   *
   * @return Text with status code
   * @throws IOException, IllegalArgumentException
   */
  @GET
  @Produces("text/tab-separated-values")
  @Path("job/runThenExportCSV/{notebookId}/paragraph/{paragraphId}-export.csv")
  public Response runThenExportTSV(@PathParam("notebookId") final String notebookId,
                                   @PathParam("paragraphId") final String paragraphId) throws
    IOException, IllegalArgumentException {
    StreamingOutput streamingOutput = new StreamingOutput() {
      @Override
      public void write(OutputStream outputStream) throws IOException, WebApplicationException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hd1001.hadoop.zbjwork.com");
        TableName tableName = TableName.valueOf("zeppelin-sql-cache");
        String path = "";
        try {
          HTable hTable = new HTable(conf, tableName);
          String rowKey = notebookId + paragraphId;
          Get get = new Get(rowKey.getBytes());
          Result rs = null;
          try {
            rs = hTable.get(get);
          } catch (IOException e) {
            e.printStackTrace();
          }

          if (rs != null) {
            List<Cell> cells = rs.listCells();
            if (cells != null) {
              for (Cell cell : cells) {
                if (new String(CellUtil.cloneQualifier(cell)).equals("path")) {
                  path = new String(CellUtil.cloneValue(cell));
                }
              }
            }
          }
          hTable.close();

          if (!path.equals("")) {
            Configuration hdfsconf = new Configuration();
            hdfsconf.addResource(new org.apache.hadoop.fs.Path("/home/hadoop/conf/hdfs-site.xml"));
            hdfsconf.addResource(new org.apache.hadoop.fs.Path("/home/hadoop/conf/core-site.xml"));
            hdfsconf.addResource(
              new org.apache.hadoop.fs.Path("/home/hadoop/conf/mapred-site.xml"));
            FileSystem fileSystem = FileSystem.get(hdfsconf);
            FSDataInputStream in = fileSystem.open(new org.apache.hadoop.fs.Path(path));
            outputStream.write((byte) 0xEF);
            outputStream.write((byte) 0xBB);
            outputStream.write((byte) 0xBF);
            byte[] buf = new byte[8096 * 100];
            int c = 0;
            while ((c = in.read(buf)) > 0) {
              outputStream.write(buf, 0, c);
              outputStream.flush();
            }
            outputStream.close();
            in.close();
            fileSystem.close();
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    return Response.ok(streamingOutput, "text/tab-separated-values").build();
  }
}
