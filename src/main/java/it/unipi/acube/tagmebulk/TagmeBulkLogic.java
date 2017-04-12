package it.unipi.acube.tagmebulk;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.gcube.common.authorization.client.exceptions.ObjectNotFound;
import org.gcube.common.homelibrary.home.HomeLibrary;
import org.gcube.common.homelibrary.home.exceptions.InternalErrorException;
import org.gcube.common.homelibrary.home.workspace.Workspace;
import org.gcube.common.homelibrary.home.workspace.WorkspaceFolder;
import org.gcube.common.homelibrary.home.workspace.WorkspaceItem;
import org.gcube.common.homelibrary.home.workspace.folder.items.ExternalFile;
import org.gcube.common.homelibrary.util.WorkspaceUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagmeBulkLogic {
	private final static Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private static final String OUTPUT_FILENAME_TEMPLATE = "tagme_annotations_%d.csv";

	public static String processBulk(HttpClient client, String tagmeUrl, String gcubeToken, String inputFileName,
	        final String outputDir) throws ObjectNotFound, Exception {
		Workspace ws = HomeLibrary.getHomeManagerFactory().getHomeManager().getHome().getWorkspace();
		ExternalFile inputFile = (ExternalFile) ws.getItemByPath(inputFileName);
		Reader inputReader = new InputStreamReader(inputFile.getData());

		PipedOutputStream resultStreamOut = new PipedOutputStream();
		PipedInputStream resultStreamIn = new PipedInputStream(resultStreamOut);

		Future<String> futureResult = Executors.newSingleThreadExecutor().submit(new ResultWriter(ws, outputDir, resultStreamIn));

		CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(resultStreamOut, "utf-8"), CSVFormat.DEFAULT.withHeader(
		        "doc-id", "timestamp", "time", "lang", "start", "end", "spot", "title", "id", "link_probability", "rho"));

		Iterable<CSVRecord> inRecords = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(inputReader);
		for (CSVRecord record : inRecords) {
			String docId = record.get("doc-id");
			String lang = record.get("lang");
			String text = record.get("text");
			sendRequest(client, tagmeUrl, gcubeToken, docId, lang, text, csvPrinter);
		}
		csvPrinter.close();
		return futureResult.get();
	}

	public static void sendRequest(HttpClient client, String tagmeUrl, String gcubeToken, String docId, String lang, String text,
	        CSVPrinter out) throws ClientProtocolException, IOException, ParseException, JSONException {
		HttpPost request = new HttpPost(tagmeUrl);
		List<NameValuePair> parameters = new ArrayList<>();
		parameters.add(new BasicNameValuePair("gcube-token", gcubeToken));
		parameters.add(new BasicNameValuePair("lang", lang));
		parameters.add(new BasicNameValuePair("text", text));
		request.setEntity(new UrlEncodedFormEntity(parameters));

		HttpResponse response = client.execute(request);
		if (response.getStatusLine().getStatusCode() != 200) {
			LOG.error("Tagme returned status code: {} {}", response.getStatusLine().getStatusCode(),
			        EntityUtils.toString(response.getEntity()));
			throw new ClientProtocolException("Tagme returned status code: " + response.getStatusLine().getStatusCode());
		}
		JSONObject responseJson = new JSONObject(EntityUtils.toString(response.getEntity()));
		String timestamp = responseJson.getString("timestamp");
		String time = responseJson.getString("time");
		JSONArray annotations = responseJson.getJSONArray("annotations");
		for (int annI = 0; annI < annotations.length(); annI++) {
			JSONObject annotation = annotations.getJSONObject(annI);
			out.printRecord(docId, timestamp, time, lang, annotation.getInt("start"), annotation.getInt("end"),
			        annotation.getString("spot"), annotation.getString("title"), annotation.getInt("id"),
			        annotation.getDouble("link_probability"), annotation.getDouble("rho"));
		}
	}

	private static class ResultWriter implements Callable<String> {
		private Workspace ws;
		private String outputDir;
		private InputStream pis;

		public ResultWriter(Workspace ws, String outputDir, InputStream pis) {
			this.ws = ws;
			this.outputDir = outputDir;
			this.pis = pis;
		}

		@Override
		public String call() throws Exception {
			WorkspaceFolder outDir = (WorkspaceFolder) ws.getItemByPath(outputDir);
			String filename = getProgressiveFilename(outDir);
			WorkspaceUtil.createExternalFile(outDir, filename, "Annotations by TagMe", pis);
			return String.format("%s/%s", outputDir, filename);
		}

	}

	private static String getProgressiveFilename(WorkspaceFolder outDir) throws InternalErrorException {
		// TODO: This is not very safe for concurrent calls.
		Set<String> existingFiles = new HashSet<>();
		for (WorkspaceItem i : outDir.getChildren())
			existingFiles.add(i.getName());
		long count = 0;
		while (existingFiles.contains(String.format(OUTPUT_FILENAME_TEMPLATE, count)))
			count++;
		return String.format(OUTPUT_FILENAME_TEMPLATE, count);
	}

}
