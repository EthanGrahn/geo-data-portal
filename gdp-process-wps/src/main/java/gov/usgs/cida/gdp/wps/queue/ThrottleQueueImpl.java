package gov.usgs.cida.gdp.wps.queue;

import gov.usgs.cida.gdp.wps.util.DatabaseUtil;
import gov.usgs.cida.gdp.wps.util.DocumentUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.n52.wps.server.ExceptionReport;
import org.n52.wps.server.request.ExecuteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.n52.wps.server.database.connection.ConnectionHandler;
import org.n52.wps.server.request.ExecuteRequestWrapper;
import org.xml.sax.SAXException;

/**
 * Implements the pre and post logic required for the processing threads
 * (ExecuteRequest) meant to protect the data resource by preventing the
 * re-running of previously finished or running requests and not running
 * multiple requests that would require the same data source to process. This is
 * not a 'real' queue in the sense that it is not inheriting from the
 * concurrency package nor using callable/runnable directly. Instead, it relies
 * heavily on the database status. ie request will go into 'waiting' status if
 * the data source is currently in use.
 *
 * @author smlarson
 */
public class ThrottleQueueImpl implements ThrottleQueue {

	private final Lock lock = new ReentrantLock();
	private final HashMap<String, ExecuteRequest> requestQueue = new HashMap();  // requestID is the key, ExecuteRequest is the value
	private final HashSet<String> dataSetInUse = new HashSet();  //debug tool
	private static final Logger LOGGER = LoggerFactory.getLogger(ThrottleQueueImpl.class);
	private static final ConnectionHandler CONNECTION_HANDLER = DatabaseUtil.getJNDIConnectionHandler();
	private static final int TIME_OUT_SECONDS = 20;

	// queries     // fyi: throttle_queue status life cycle: ACCEPTED<insert>—> PREENQUEUE then ENQUEUE <update>with time—>STARTED<update> —>PROCESSED<update>
	private static final String THROTTLE_QUEUE_TABLE = "throttle_queue";
	private static final String SELECT_RESPONSE_URL_STATEMENT = "SELECT response FROM results, response WHERE results.request_id = response.request_id and response.status = 'SUCCEEDED' and response.request_id = ?";
	private static final String INSERT_STATUS_REQUEST_STATEMENT = "INSERT INTO " + THROTTLE_QUEUE_TABLE + "(REQUEST_ID, STATUS, ENQUEUED, DEQUEUED) VALUES (?, ?, ?, ?)";
	private static final String UPDATE_STATUS_DEQUEUE_STATEMENT = "UPDATE " + THROTTLE_QUEUE_TABLE + " SET (STATUS, DEQUEUED) = ROW(?, ?) WHERE REQUEST_ID = ?";
	private static final String UPDATE_STATUS_ENQUEUE_STATEMENT = "UPDATE " + THROTTLE_QUEUE_TABLE + " SET (STATUS, ENQUEUED) = ROW(?, ?) WHERE REQUEST_ID = ?";
	private static final String UPDATE_STATUS_STATEMENT = "UPDATE " + THROTTLE_QUEUE_TABLE + " SET (STATUS) = ROW(?) WHERE REQUEST_ID = ?";
	private static final String SELECT_DATA_SOURCES_STATEMENT = "SELECT INPUT_VALUE FROM INPUT WHERE INPUT_IDENTIFIER = 'DATASET_URI' AND REQUEST_ID = ?";
	private static final String UPDATE_ONE_WORK_RETURNING = "UPDATE throttle_queue q SET STATUS = 'PREENQUEUE' FROM "
			+ "(Select input.request_id, input_value FROM input input, throttle_queue throttle WHERE input.input_identifier = 'DATASET_URI' AND input.input_value NOT IN "
			+ "(Select DISTINCT(INPUT_VALUE) FROM input input, response resp, throttle_queue throttle WHERE resp.request_id = input.request_id AND resp.request_id = throttle.request_id  "
			+ "AND input.INPUT_IDENTIFIER = 'DATASET_URI' AND (resp.status = 'STARTED' OR throttle.status = 'ENQUEUE')) AND throttle.request_id = input.request_id AND throttle.status = 'ACCEPTED' "
			+ "ORDER BY enqueued ASC LIMIT 1) sub WHERE q.request_id = sub.request_id RETURNING q.request_id";

	private static final String SELECT_REQUEST_XML = "SELECT request_xml FROM request WHERE request_id = ?";


	/**
	 * Ensures the same request is not processed again (either finished or
	 * executing). As a pre-processing check, first check to see if it has ran
	 * or is currently running and hand back the status url if it is. Add the
	 * request to the hashmap and set the status to ACCEPTED
	 *
	 *
	 * @param req
	 * @throws org.n52.wps.server.ExceptionReport
	 */
	@Override
	public void putRequest(ExecuteRequest req) throws ExceptionReport {

		try {
			lock.tryLock(TIME_OUT_SECONDS, TimeUnit.SECONDS);   //#TODO# review timeout duration 
			UUID requestId = req.getUniqueId();

			if (isRedundantRequestEnabled()) {  //can toggle this logic on and off with the setting in the DB - throttle_queue_toggle table
				if (hasRan(req)) {
					LOGGER.debug("Current request has ran previously. Getting file response URI...");
					String responseId = getPreviousResponseId(requestId);
					LOGGER.debug("Response ID, " + responseId + " found for request with ID..." + requestId.toString());
				} else {
					addRequest(req);
				}
			} else {
				// move this to a an add request to easily manage the put of the datasets too
				LOGGER.debug("Adding request to Throttle Queue with ID:" + requestId.toString());
				addRequest(req);
			}

			/// record status in DB (separate status from the wps status)  ACCEPTED
		} catch (InterruptedException e) {
			LOGGER.debug("Attempt to acquire lock to add request to the throttle timed out: " + e.getMessage());

		} finally {
			lock.unlock();
		}
	}

	/**
	 * Ensures the same dataset is currently not in use before updating the
	 * status and giving the worker thread the doc to execute upon
	 * 'requestQueue'. 1) updates DB status to PROCESSED 2) removes the data
	 * resource from the set 3) removes the request from the internal hashmap
	 *
	 * @param requestId
	 * @throws org.n52.wps.server.ExceptionReport
	 */
	@Override
	public void removeRequest(String requestId) throws ExceptionReport {

		try {
			lock.tryLock(TIME_OUT_SECONDS, TimeUnit.SECONDS);   //#TODO# review timeout duration 

			if (null == requestId) {
				LOGGER.debug("There are no requests that are available for processing at this time....");
				//result = null;

			} else {

				updateQueueStatusProcessed(requestId);

				//remove the inuse data sources
				this.dataSetInUse.removeAll(this.getDataSources(requestId));

				//remove the request from the Hashmap
				this.requestQueue.remove(requestId);
				LOGGER.debug("Removed request from queue:" + requestId);
			}

		} catch (InterruptedException e) {
			LOGGER.debug("Attempt to acquire lock to remove the request from the throttle timed out: " + e.getMessage());

		} finally {
			lock.unlock();
		}

	}

	// this affects the DB (status ACCEPTED) and internal maps/sets, it does not place the work on the 'queue'
	private void addRequest(ExecuteRequest req) throws ExceptionReport {
		String requestId = req.getUniqueId().toString();

		this.requestQueue.put(requestId, req); //adds it to the internal hashmap for easy retrieval
		LOGGER.debug("Added request:" + requestId + "to queue hashmap.");

		//add the dataresources needed to process this request to the inUse set
		addDataResources(requestId);

		insertQueueRequest(requestId);//ACCEPTED

	}

	/**
	 * Used to protect the data resource: If something is currently utilizing
	 * the data resource, this request will be blocked until the other request
	 * completes and the data resource is available.
	 *
	 */
	//add the data resources to the internal hash set by fetching them from the DB
	private void addDataResources(String requestId) throws ExceptionReport {
		List<String> resources = getDataSources(requestId);
		this.dataSetInUse.addAll(resources);

		for (String resource : resources) {
			LOGGER.debug("Added resources to inUse hashset:" + resource);
		}
	}

	// marks the request as having been queued in the throttle_queue db
	/**
	 * Updates the throttle_queue table with the status given. Note that enqueue
	 * and dequeue utilize timestamps. Enqueue may go to waiting and back to
	 * enqueue more than once.
	 *
	 * @param exec ExecuteRequest
	 * @param status throttle status (not wps status)
	 * @throws ExceptionReport
	 */
	@Override
	public void updateStatus(ExecuteRequest exec, ThrottleStatus status) throws ExceptionReport {
		if (null != status) {
			switch (status) {
				case ENQUEUE:
					LOGGER.debug("Status recieved as ENQUEUE.");
					updateQueueEnqueue(exec.getUniqueId().toString());
					break;
				case WAITING:
					LOGGER.debug("Status recieved as WAITING.");
					updateStatusWaiting(exec.getUniqueId().toString());
					break;
				case STARTED:
					LOGGER.debug("Status recieved as STARTED.");
					updateStatusStarted(exec.getUniqueId().toString());
					break;
				default:
					break;
			}
		}
	}

	@Override
	public boolean isEnqueue(ExecuteRequest req) throws ExceptionReport {
		boolean result = false;
		try (Connection connection = CONNECTION_HANDLER.getConnection()) {

			String sql = "SELECT request_id FROM throttle_queue  WHERE status = '" + ThrottleStatus.ENQUEUE.toString() + "'AND request_id = ?";
			PreparedStatement statement = connection.prepareStatement(sql);
			statement.setString(1, req.getUniqueId().toString());

			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				String id = rs.getString("request_id");
				if (null != id) {
					result = true;
				}
			}
		} catch (SQLException ex) {
			String msg = "Problem selecting throttle_queue enqueue status for reqId:" + req.getUniqueId().toString();
			LOGGER.error(msg, ex);
			LOGGER.debug(msg);
			throw new ExceptionReport(msg, ExceptionReport.NO_APPLICABLE_CODE);
		}
		return result;
	}

	private void updateStatusStarted(String requestId) throws ExceptionReport {
		ThrottleStatus status = ThrottleStatus.STARTED;

		try (Connection connection = CONNECTION_HANDLER.getConnection()) {
			PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_STATUS_STATEMENT);

			preparedStatement.setString(1, status.toString());
			preparedStatement.setString(2, requestId);

			preparedStatement.executeUpdate();
			LOGGER.debug("Updated status in throttle_queue table of request:" + requestId + " to status of :" + status.toString());
		} catch (Exception e) {
			String msg = "Failed to update status to STARTED on throttle_queue table. :" + UPDATE_STATUS_STATEMENT;
			LOGGER.error(msg, e);
			throw new ExceptionReport(msg, ExceptionReport.NO_APPLICABLE_CODE);
		}
	}

	private void updateStatusWaiting(String requestId) throws ExceptionReport {
		ThrottleStatus status = ThrottleStatus.WAITING;

		try (Connection connection = CONNECTION_HANDLER.getConnection()) {
			PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_STATUS_STATEMENT);

			preparedStatement.setString(1, status.toString());
			preparedStatement.setString(2, requestId);

			preparedStatement.executeUpdate();
			LOGGER.debug("Updated status in throttle_queue table of request:" + requestId + " to status of :" + status.toString());
		} catch (Exception e) {
			String msg = "Failed to update status to STARTED on throttle_queue table. :" + UPDATE_STATUS_STATEMENT;
			LOGGER.error(msg, e);
			throw new ExceptionReport(msg, ExceptionReport.NO_APPLICABLE_CODE);
		}
	}

	// #PRE_CHECK#  // 7/2016 blocked on requirement: will a pre-sort of the inputs be needed to satisfy sameness?
	private boolean hasRan(ExecuteRequest req) {
		// "TBD: impl later";
		Document currentRequestDoc = req.getDocument();
		boolean result = false;

		// if hasRan or isRunning - result = true
		// determine this by comparing a checksum (MD5) on the doc (request_xml) found in the request (table).request_xml and this requests doc
		// String thisRequest = MD5(currentRequestDoc);
		// get the one from the DB that matches the DB size 
		// String compareTo = MD5(dbRequestDoc);
		// compare the two strings to see if they .equal, possibly looping thru them if more than one with a matched size are returned from the db
		LOGGER.debug("Pre-check before placing on queue -hasRanBefore:" + result);
		return result; // update later after impl
	}

	// #PRE_CHECK#
	private String MD5(String md5) throws ExceptionReport {

		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] array = md.digest(md5.getBytes(Charset.forName("UTF-8")));
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < array.length; ++i) {
				sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
			}
			return sb.toString();
		} catch (java.security.NoSuchAlgorithmException e) {
			String msg = "Can not verify if request has been processed previously. MD5 not available.";
			LOGGER.debug("MD5 algo not available.");
			throw new ExceptionReport(msg, ExceptionReport.NO_APPLICABLE_CODE);
		}
	}

	// #PRE_CHECK#
	private String getPreviousResponseId(UUID currentRequestId) throws ExceptionReport {
		String resultURL = null;

		//return the result (table).response(column) where the response (table).status(column) is succeeded for the currentRequestId
		try (Connection connection = CONNECTION_HANDLER.getConnection();
				PreparedStatement selectRequestStatement = connection.prepareStatement(SELECT_RESPONSE_URL_STATEMENT)) {
			selectRequestStatement.setString(1, currentRequestId.toString());

			ResultSet rs = selectRequestStatement.executeQuery();

			if (rs != null && rs.next()) {
				resultURL = rs.getString(1);
				LOGGER.debug("Got previous responsId for request:" + currentRequestId.toString());
			}
		} catch (Exception e) {
			String msg = "Failed to find previously ran response URL from database.";
			LOGGER.error(msg, e);
			throw new ExceptionReport(msg, ExceptionReport.NO_APPLICABLE_CODE);
		}
		return resultURL;

	}

	private boolean isRedundantRequestEnabled() throws ExceptionReport {
		boolean result = true;
		try (Connection connection = CONNECTION_HANDLER.getConnection()) {

			String sql = "SELECT enabled FROM throttle_queue_toggle  WHERE toggle_type = " + "'" + ToggleType.REDUNDANT_REQUEST_CHECK.toString().toUpperCase() + "'"; // \"REDUNDANT_REQUEST_CHECK\"";
			Statement statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(sql);
			while (rs.next()) {
				String boolString = rs.getString("ENABLED");
				result = Boolean.valueOf(boolString);
			}
		} catch (SQLException ex) {
			LOGGER.error("Problem selecting throttle_queue_toggle for pre-check redundant request enabled from the db.", ex);
			LOGGER.debug("Throttle_queue reundant request enabled: " + result);
			throw new ExceptionReport("Problem selecting throttle_queue_toggle enabled from the db. Redundant request 'enabled' is currently:" + result, ExceptionReport.NO_APPLICABLE_CODE);
		}
		return result;
	}

	//gets the datasources from the input table instead of parsing the doc cuz its more convenient now that they are there for the dequeueing process
	private List getDataSources(String requestId) throws ExceptionReport {
		//select distinct(input_value) from input where input_identifier = 'DATASET_URI';  //list of all the potential datasources in use
		List<String> result = new ArrayList<>();  //must maintain order - FIFO

		try (Connection connection = CONNECTION_HANDLER.getConnection();
				PreparedStatement selectRequestStatement = connection.prepareStatement(SELECT_DATA_SOURCES_STATEMENT)) {
			selectRequestStatement.setString(1, requestId);

			ResultSet rs = selectRequestStatement.executeQuery();

			if (rs != null) {
				while (rs.next()) {
					result.add(rs.getString(1));
					LOGGER.debug("Fetched datasources from input for requestId:" + requestId + " datasource: " + rs.getString(1));
				}
			}
		} catch (Exception e) {
			String msg = "Failed to find previously ran response URL from database";
			LOGGER.error(msg, e);
			throw new ExceptionReport(msg, ExceptionReport.NO_APPLICABLE_CODE);
		}
		return result;
	}

	private void insertQueueRequest(String requestId) throws ExceptionReport {
		//ACCEPTED status upon insert

		Timestamp now = new Timestamp(Calendar.getInstance().getTimeInMillis());

		try (Connection connection = CONNECTION_HANDLER.getConnection()) {
			PreparedStatement preparedStatement = connection.prepareStatement(INSERT_STATUS_REQUEST_STATEMENT);
			preparedStatement.setString(1, requestId);
			preparedStatement.setString(2, ThrottleStatus.ACCEPTED.toString());
			preparedStatement.setTimestamp(3, now);
			preparedStatement.setTimestamp(4, null); //dont want to set the dequeue time now

			preparedStatement.execute();
			LOGGER.debug("Inserted request in throttle_queue table with ID:" + requestId + " with status of ENQUEUE.");
		} catch (Exception e) {
			String msg = "Failed to insert request and status into throttle_queue table.";
			LOGGER.error(msg, e);
			throw new ExceptionReport(msg, ExceptionReport.NO_APPLICABLE_CODE);
		}
	}

	//update the requests status on the throttle_queue table for the dequeuing process
	private void updateQueueStatusProcessed(String requestId) throws ExceptionReport {
		//PREENQUEUE / ENQUEUE -> STARTED -> PROCESSED 
		Timestamp now = new Timestamp(Calendar.getInstance().getTimeInMillis());
		ThrottleStatus status = ThrottleStatus.PROCESSED;

		try (Connection connection = CONNECTION_HANDLER.getConnection()) {
			PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_STATUS_DEQUEUE_STATEMENT);

			preparedStatement.setString(1, status.toString());
			preparedStatement.setTimestamp(2, now);  //dequeue time
			preparedStatement.setString(3, requestId);

			preparedStatement.executeUpdate();
			LOGGER.debug("Updated status in throttle_queue table of request:" + requestId + " to status of :" + status.toString());
		} catch (Exception e) {
			String msg = "Failed to update status to STARTED on throttle_queue table. :" + UPDATE_STATUS_DEQUEUE_STATEMENT;
			LOGGER.error(msg, e);
			throw new ExceptionReport(msg, ExceptionReport.NO_APPLICABLE_CODE);
		}
	}

	//update the requests status on the throttle_queue table for the queuing process from the thread after its started
	private void updateQueueEnqueue(String requestId) throws ExceptionReport {
		//ENQUEUE -> STARTED -> PROCESSED
		ThrottleStatus status = ThrottleStatus.ENQUEUE;
		Timestamp now = new Timestamp(Calendar.getInstance().getTimeInMillis());

		try (Connection connection = CONNECTION_HANDLER.getConnection()) {
			PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_STATUS_ENQUEUE_STATEMENT);

			preparedStatement.setString(1, status.toString());
			preparedStatement.setTimestamp(2, now);
			preparedStatement.setString(3, requestId);

			preparedStatement.executeUpdate();
			LOGGER.debug("Updated status in throttle_queue table of request:" + requestId + " to status of :" + status.toString());
		} catch (Exception e) {
			String msg = "Failed to update status to ENQUEUE on throttle_queue table. :" + UPDATE_STATUS_ENQUEUE_STATEMENT;
			LOGGER.error(msg, e);
			throw new ExceptionReport(msg, ExceptionReport.NO_APPLICABLE_CODE);
		}
	}

	/**
	 * Enqueues the work from the db found with a status of ACCEPTED on the
	 * throttle queue. This is called from a timer task held onto by the
	 * RequestManager. Forces a separation between processing the requests and
	 * any pre-check logic ie if the request has been made before or if the
	 * request's data source is in-use.
	 *
	 * @throws org.n52.wps.server.ExceptionReport
	 */
	@Override
	public void enqueueRemainingWork() throws ExceptionReport {
		try {
			lock.tryLock(TIME_OUT_SECONDS, TimeUnit.SECONDS);
			//triggers the RM to return the resources and see if there is more work to be done

			LOGGER.debug("Enqueing remaining work...");
			// getRemainingWork and add it to the queue
			List<String> requestIds = getRemainingWork();

			if (null != requestIds && requestIds.size() > 0) {
				LOGGER.debug("Quantity of work to be addeded to queue:" + requestIds.size());
				addWorkToQueue(requestIds);
			} else {
				LOGGER.debug("No additional work has been found for the queue.");
			}

		} catch (InterruptedException e) {
			LOGGER.debug("Attempt to acquire lock to add request to the throttle timed out: " + e.getMessage());

		} finally {
			lock.unlock();
		}

	}

	/* this can be used to restart work, might need minor tweaks */
	private void addWorkToQueue(List<String> requestIds) throws ExceptionReport {

		for (String id : requestIds) {

			try {
				//need to get the ExecuteRequest from the db and re-create the wrapped request
				LOGGER.debug("Creating doc and request so that work can be added to throttle queue: " + id);
				String xml = getDoc(id);  //SELECT_REQUEST_XML
				Document doc = DocumentUtil.createDocument(new ByteArrayInputStream(xml.getBytes("UTF-8")));
				// add to the pool ie RequestHandler
				ExecuteRequestWrapper req = new ExecuteRequestWrapper(doc);
				//must reset the id to the previously stored id that occurred when the request was 'ACCEPTED'
				LOGGER.debug("Setting id on ExecuteRequest." + id);
				req.setId(UUID.fromString(id)); //overlay the id with previous ACCEPTED id

				ExecuteRequestManager.getInstance().getExecuteRequestQueue().put(req); //adds the request to the queue

			} catch (SAXException | IOException ex) {
				String msg = "Error in parsing doc for setting up remaining work on queue.";
				LOGGER.debug(msg);
				throw new ExceptionReport(msg, ExceptionReport.NO_APPLICABLE_CODE);
			}
		}
	}

	private String getDoc(String id) throws ExceptionReport {
		String result = null;
		// check the throttle_queue table for any requests that are in 'ACCEPTED' status
		// may also want to get all that are marked as STARTED with a dequeued time of older than 24 hours as it is most likely in a bad state #TODO# verify
		try (Connection connection = CONNECTION_HANDLER.getConnection();
				PreparedStatement selectRequestStatement = connection.prepareStatement(SELECT_REQUEST_XML)) {
			selectRequestStatement.setString(1, id);

			ResultSet rs = selectRequestStatement.executeQuery();

			if (rs != null) {
				while (rs.next()) {
					result = rs.getString("request_xml");
					LOGGER.debug("Fetched doc from request with request id:" + id);
				}
			}
		} catch (Exception e) {
			String msg = "Failed to find xml doc needed to add remaining work to queue.";
			LOGGER.error(msg, e);
			throw new ExceptionReport(msg, ExceptionReport.NO_APPLICABLE_CODE);
		}

		return result;
	}

	private List<String> getRemainingWork() throws ExceptionReport {
		List<String> result = new ArrayList(10);
		// check the throttle_queue table for any requests that are in 'ACCEPTED' status who's data resource is not in use
		// limited to bringing back the (as in one) oldest request - could easily handle a list 
		try (Connection connection = CONNECTION_HANDLER.getConnection();
				PreparedStatement selectRequestStatement = connection.prepareStatement(UPDATE_ONE_WORK_RETURNING)) {

			ResultSet rs = selectRequestStatement.executeQuery();

			if (rs != null) {
				while (rs.next()) {
					result.add(rs.getString("request_id"));
					LOGGER.info("Fetched work from throttle_queue with request id:" + rs.getString(1));
				}
			}
		} catch (Exception e) {
			String msg = "Failed to execute query to find remaining work from throttle_queue.";
			LOGGER.error(msg, e);
			throw new ExceptionReport(msg, ExceptionReport.NO_APPLICABLE_CODE);
		}

		return result;
	}
}
