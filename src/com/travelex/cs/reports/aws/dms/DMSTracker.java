package com.travelex.cs.reports.aws.dms;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DMSTracker implements
		RequestHandler<Map<String, LinkedHashMap<String, String>>, String> {

	private LambdaLogger sout = null;
	HashMap<String, Notification> notifications = new HashMap<>();
	Notification.Builder nbuilder;
	List<String> migrations = new ArrayList<>();

	@Override
	public String handleRequest(
			Map<String, LinkedHashMap<String, String>> input, Context context) {
		sout = context.getLogger();
		LinkedHashMap<String, String> data = input.get("awslogs");
		String logs = data.get("data");
		sout.log("base64encodedAndgzippedData - " + logs);
		byte[] base64decoded = Base64.getDecoder().decode(
				logs.getBytes(StandardCharsets.UTF_8));
		String cwlogInfo = null;
		try {
			// the data should be in gzip format, check that
			if (isGZipCompressed(base64decoded)) {
				cwlogInfo = new String(decompress(base64decoded));
				sout.log("cwlogs=>" + cwlogInfo);
				ObjectMapper mapper = new ObjectMapper();
				JsonNode node = mapper.readTree(cwlogInfo);
				Consumer<? super JsonNode> notify = root -> {
					buildNotification(root);
				};
				node.path("logEvents").elements().forEachRemaining(notify);
				// what about the last migration task
				if (migrations.size() - notifications.size() == 1) {
					buildANotification();
				}

				for (String migration : migrations) {
					Notification migrationTask = notifications
							.remove(migration);
					sout.log("migrationTask=>" + migrationTask);
					String stopReason = migrationTask.getStopReason();

					if (!StringUtils.isNullOrEmpty(stopReason)
							&& stopReason
									.contains(Notification.FULL_LOAD_ONLY_FINISHED)) {
						execute(migrationTask);
					}
				}
			}

		} catch (Exception e) {
			sout.log("gzip decompress failed on cloud watch logs for dms task monitor - "
					+ e.getMessage());
			e.printStackTrace();
		}
		return cwlogInfo;
	}

	private void buildNotification(JsonNode root) {
		List<String> keys = Arrays.asList("migrationEndpoint=",
				"timeOfExecution=", "replicationTaskStartDate=",
				"replicationTaskStartedToday=", "executionStatus=",
				"stopReason=", "replicationTaskStats=", "notificationsql=",
				"result=");
		String message = root.path("message").textValue();
		sout.log("message=>" + message);
		keys.forEach(key -> {
			int keyIdx = message.indexOf(key);
			boolean keyPresent = keyIdx != -1;
			if (keyPresent) {
				String value = message.substring(keyIdx + key.length());
				sout.log("key=>" + key);
				sout.log("value=>" + value);
				if (key.equalsIgnoreCase("migrationEndpoint=")) {
					// the first migrationTask is complete && continues until
					// last but one
					migrations.add(value);
					if (nbuilder != null) {
						buildANotification();
					}
					nbuilder = new Notification.Builder();
				}
				build(key, value);
			}
		});
	}

	private void buildANotification() {
		Notification taskNotification = nbuilder.build();
		notifications.put(taskNotification.getMigrationEndpoint(),
				taskNotification);
	}

	private void build(String key, String value) {
		if (value == null)
			return;
		switch (key.toLowerCase()) {
		case "migrationendpoint=":
			nbuilder.migrationEndPoint(value);
			break;
		case "timeofexecution=":
			nbuilder.timeOfExecution(value);
			break;
		case "replicationtaskstartdate=":
			nbuilder.replicationTaskStartDate(value);
			break;
		case "replicationtaskstartedtoday=":
			nbuilder.replicationTaskStartedToday(value);
			break;
		case "executionstatus=":
			nbuilder.executionStatus(value);
			break;
		case "stopreason=":
			nbuilder.stopReason(value);
			break;
		case "replicationtaskstats=":
			nbuilder.replicationTaskStats(value);
			break;
		case "notificationsql=":
			nbuilder.notificationsql(value);
			break;
		case "result=":
			nbuilder.result(value);
			break;
		}
	}

	public String decompress(byte[] data) throws Exception {
		GZIPInputStream gis = new GZIPInputStream(
				new ByteArrayInputStream(data));
		BufferedReader bf = new BufferedReader(new InputStreamReader(gis,
				StandardCharsets.UTF_8));
		String outStr = "";
		String line;
		while ((line = bf.readLine()) != null) {
			outStr += line;
		}
		return outStr;
	}

	private boolean isGZipCompressed(byte[] bytes) throws IOException {
		if ((bytes == null) || (bytes.length < 2)) {
			return false;
		} else {
			return ((bytes[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) && (bytes[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8)));
		}
	}

	public static void main(String[] args) throws Exception {
		DMSTracker tracker = new DMSTracker();
		String base64EncodedAndCompressedString = "H4sIAAAAAAAAAK2SW2/aQBCF/8rK6lshzF496zciaFQptBW4TyGKFntDLbBN10uTNMp/7xh6iSpVohKPPjM755vjeU5q33Vu7fOnnU+yZDLOx3ez6WIxvpomg6R9aHwgWYIxkgOkSluSt+36KrT7HVVG7qEbbV29Kt2ortbBxaptctdtFtHFfTdzDQ0PxzeLGLyr6ZEAno5AjgBHN2+ux/l0kd+CFHgv1D1Ht1JFaVZao11JtCVYJ7SjEd1+1RWh2vUW76pt9KFLspvk+uB+HH5X1l0eXLEhz9uD6fSbb2Lf95xUZb+KFKC5lBo1aGU0cq6UoA+y4jYVXAgtETQqUgw3ioPUaWrIP1YUVnQ17c0VojXWINXV4FeINL744osN+50E6w4xLJvkZfA3gFXSoklpvDBcoFQWUaS0NiqrhEUwKfUaZVACIP8XAEr5GiD43bYqXv+GECcuetaHPgQ5BGQcMqkybS4w1QDwFiADOAOjOJExtqV7egV0Bmt5onWfCYuP9V3Rhl0b+mDCvmmqZn0GCHUixPTDhM391z01vi8zVqRlkcK9ozD8asi5T4doNQwVp/NEh6IQ9gx0+kS6+fTTx3n+34Bxsj/efMYU4IW1rO6W8bLabn3J/tTo4KjAlnHm6zY8sUX13WeM6NnskkT3yH4WPneerKU+6P36ty8/APNvzmeuBAAA";
		byte[] encodedAndCompressedByteStream = base64EncodedAndCompressedString
				.getBytes(StandardCharsets.UTF_8);
		byte[] decodedButStillCompressedByteStream = Base64.getDecoder()
				.decode(encodedAndCompressedByteStream);
		boolean gZipCompressed = tracker
				.isGZipCompressed(decodedButStillCompressedByteStream);
		System.out.println("gZipCompressed = " + gZipCompressed);
		if (gZipCompressed) {
			System.out.println(tracker
					.decompress(decodedButStillCompressedByteStream));
		}
		System.out.println("LocalDateTime.now().toString() is "
				+ LocalDateTime.now().toString());
	}

	private void execute(Notification notify) {
		Connection con = null;
		Statement statement = null;
		ResultSet rs = null;

		try {
			con = makeConnection();
			String todaysEntrySql = "select 1 from dms_task_log "
					+ "where database_name = '"
							.concat(notify.getMigrationEndpoint())
							.concat("' and etl_date like '%")
							.concat(notify.getTimeOfExecution()
									.substring(0, 11).concat("%'"));
			String sql = "insert into dms_task_log "
					+ "(etl_date, database_name, task_start, task_statistics,"
					+ " mibi_load_completed, last_updated)"
					+ " values (?, ?, ?, ?, ?, ?)";
			sout.log("existing entry sql = " + todaysEntrySql);
			ResultSet results = con.createStatement().executeQuery(
					todaysEntrySql);
			if (results.next() == true) {
				sout.log("The end point - ".concat(
						notify.getMigrationEndpoint()).concat(
						" is already in, so not inserting it again"));
				return;
			}
			sout.log("the sql - " + sql);
			PreparedStatement p = con.prepareStatement(sql);
			p.setString(1, notify.getTimeOfExecution());
			p.setString(2, notify.getMigrationEndpoint());
			p.setString(3, notify.getReplicationTaskStartDate());
			p.setString(4, notify.getReplicationTaskStats());
			p.setString(5, notify.getJobResultStatus());
			p.setString(6, LocalDateTime.now().toString());
			sout.log("data inserted - " + p.execute());
		}
		// Handle any errors that may have occurred.
		catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (statement != null)
					statement.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private Connection makeConnection() throws URISyntaxException, SQLException {
		String dbURI = System.getenv("dbURI");
		if (StringUtils.isNullOrEmpty(dbURI)) {
			String sampledbURI = "jdbc:sqlserver://10.234.3.130:1433;databaseName=RDS_RAP;user=AWSuser;password=Travelex1";
			throw new InvalidParameterException(
					"Missing Systm environment variable dbURI = " + sampledbURI);
		}
		String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		URI connectionURI = new URI(dbURI);
		Connection con = connectToDB(connectionURI, driver);
		sout.log("Connection is available @ " + con);
		sout.log("Connection details are "
				+ con.getMetaData().getURL().toString());
		return con;
	}

	private Connection connectToDB(URI connectionURI, String driver)
			throws URISyntaxException {
		if (connectionURI == null || StringUtils.isNullOrEmpty(driver))
			return null;
		Connection con = null;
		try {
			Class.forName(driver);
			con = DriverManager.getConnection(connectionURI.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return con;
	}
}
