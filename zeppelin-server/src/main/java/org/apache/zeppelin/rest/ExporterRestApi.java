package org.apache.zeppelin.rest;

/**
 * @author Ethan
 * @comment
 * @date 16/7/1
 */

  import org.apache.zeppelin.interpreter.InterpreterResult;
  import org.apache.zeppelin.notebook.Note;
  import org.apache.zeppelin.notebook.Notebook;
  import org.apache.zeppelin.notebook.Paragraph;
  import org.apache.zeppelin.server.JsonResponse;
  import org.slf4j.Logger;
  import org.slf4j.LoggerFactory;

  import javax.ws.rs.*;
  import javax.ws.rs.core.Response;
  import javax.ws.rs.core.Response.Status;
  import java.io.IOException;

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
  public Response runThenExportTSV(
                                   @PathParam("msg") String msg) throws
    IOException, IllegalArgumentException {
    return Response.ok(TsvToCSV.toCSV(msg)).build();
  }
}
