package it.unipi.acube.tagmebulk.servlet;

import java.lang.invoke.MethodHandles;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.client.HttpClient;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unipi.acube.tagmebulk.TagmeBulkLogic;

/**
 * @author Marco Cornolti
 */

@Path("/")
public class TagmeBulkServlet {
	private final static Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	@Context
	ServletContext context;

	@GET
	@Path("/bulk-annotate")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response bulkAnnotate(@QueryParam("gcube-token") String gcubeToken, @QueryParam("input-csv-file") String inputFileName, @QueryParam("output-csv-dir") String outputDir) {
		
		if (gcubeToken == null)
			return Response.serverError().entity("Parameter gcube-token required.").build();
		if (inputFileName == null)
			return Response.serverError().entity("Parameter input-csv-file required.").build();
		if (outputDir == null)
			return Response.serverError().entity("Parameter output-csv-dir required.").build();

		String tagmeUrl = (String) context.getAttribute("tagme-url");
		HttpClient httpClient = (HttpClient) context.getAttribute("http-client");
		
		try {
			String outFilename = TagmeBulkLogic.processBulk(httpClient, tagmeUrl, gcubeToken, inputFileName, outputDir);
			return Response.ok(getJsonResponse("Annotation finished.", outFilename, true)).build();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(String.format("Error for user-token={}, input-file={} output-file={} error={}", gcubeToken, inputFileName, outputDir, ExceptionUtils.getStackTrace(e)));
			return Response.serverError().entity(getJsonResponse(e.toString(), null, false)).build();
		}
	}
	
	public String getJsonResponse(String message, String outFile, boolean success){
		JSONObject res = new JSONObject();
		try {
			res.put("result", success ? "OK": "ERROR");
			res.put("msg", message);
			if (outFile != null)
				res.put("out-file", outFile);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return res.toString();
	}
}
