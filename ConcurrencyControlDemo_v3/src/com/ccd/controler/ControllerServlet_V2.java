package com.ccd.controler;

import java.io.IOException;
import java.util.*;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.ccd.model.OptimisticConcurrencyControlBOCC;
import com.ccd.model.OptimisticConcurrencyControlFOCC;
import com.ccd.model.TimestampOrdering;
import com.ccd.model.TwoPhaseLocking_V2;

/**
 * Servlet implementation class ControllerServlet
 */
public class ControllerServlet_V2 extends HttpServlet {
	private static final long serialVersionUID = 1L;

	HttpSession session;

	public HashMap<String, Long> sessonTranPair = new HashMap<String, Long>();
	public static ArrayList<Long> tranIDList = new ArrayList<Long>();

	// *************** 2PL *************************//
	public static HashMap<String, Double> transTable2PL = new HashMap<>();
	public static HashMap<String, Long> lockTable = new HashMap<>();
	public static HashMap<String, Double> expressionResultStorage2PL = new HashMap<>();
	public static LinkedHashMap<String, Double> rollbackTable2PL = new LinkedHashMap<>();
	public static HashMap<Long, Boolean> unlockStart = new HashMap<>();
	public static HashMap<String, Long> finalWrite2PL = new HashMap<>();
	public static HashMap<String, Long> readFromRelation2PL = new HashMap<>();
	public static HashMap<Long, String> abortedTransactionsList2PL = new HashMap<>();

	// **************** Time-stamp Ordering **************//
	public static HashMap<Long, Long> transTimeStamp = new HashMap<>();
	public static HashMap<String, Double> transTableTO = new HashMap<>();
	public static HashMap<String, Double> expressionResultStorageTO = new HashMap<>();
	public static LinkedHashMap<String, Double> rollbackTableTO = new LinkedHashMap<>();
	public static LinkedHashMap<String, Long> operationTimeStamp = new LinkedHashMap<>();
	public static HashMap<String, Long> readFromRelationTO = new HashMap<>();
	public static HashMap<String, Long> finalWriteTO = new HashMap<>();
	public static HashMap<Long, String> abortedTransactionsListTO = new HashMap<>();

	// ********************* BOCC ***************************//
	public static HashMap<String, Double> transTableBOCC = new HashMap<>();
	public static HashMap<String, Double> expressionResultStorageBOCC = new HashMap<>();
	public static HashMap<String, Double> privateWorkspaceBOCC = new HashMap<>();
	public static LinkedHashMap<Long, String> transactionPhaseBOCC = new LinkedHashMap<>();
	public static HashMap<String, Long> transLifeTime = new HashMap<>();
	public static LinkedHashMap<String, Long> readSetBOCC = new LinkedHashMap<>();
	public static LinkedHashMap<String, Long> writeSetBOCC = new LinkedHashMap<>();

	// ********************* FOCC ***************************//
	public static HashMap<String, Double> transTableFOCC = new HashMap<>();
	public static HashMap<String, Double> expressionResultStorageFOCC = new HashMap<>();
	public static HashMap<String, Double> privateWorkspaceFOCC = new HashMap<>();
	public static LinkedHashMap<String, Long> readSetFOCC = new LinkedHashMap<>();
	public static LinkedHashMap<String, Long> writeSetFOCC = new LinkedHashMap<>();
	public static LinkedHashMap<Long, String> transactionPhaseFOCC = new LinkedHashMap<>();
	public static long conflictingTranID;

	public static Object lock = new Object();

	long tranID;

	// incremented every time servlet's doPost method is executed
	private int nmbrOfCallsToServlet;

	/**
	 * @see HttpServlet#init
	 */
	public void init() {
		tranID = 1;
		nmbrOfCallsToServlet = 0;
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		// this will collect the result of each operation coming as request
		ArrayList<String> result = new ArrayList<>();

		// this attribute is used by the jsp for displaying the data. It is set every
		// time nmbrOfCallsToServlet is incremented
		request.setAttribute("counter", nmbrOfCallsToServlet);

		// set the dispatcher to the jsp page, so that request can be forwarded to the
		// jsp with all the session attributes to display
		RequestDispatcher dispatcher = request.getRequestDispatcher("ConcurrencyControlDemo_v3.jsp");

		// get the session
		session = request.getSession();

		// extract the algorithm chosen by the user
		String algorithm = request.getParameter("algorithm");

		// set a transactionID to the current session
		if (!sessonTranPair.containsKey(session.getId())) {
			tranIDList.add(tranID);
			sessonTranPair.put(session.getId(), tranID++);
		}

		// set some session attributes to handle subsequent requests
		if (algorithm != null) {
			session.setAttribute("algo", algorithm);
			unlockStart.put(sessonTranPair.get(session.getId()), false);
			session.setAttribute("tranID", sessonTranPair.get(session.getId()));
		}

		// extract algorithm name from session attribute "algo" to be used later to call
		// the respective class to handle each operation
		String currentAlgorithm = (String) session.getAttribute("algo");

		// ts will hold the operation sent via request from jsp
		ArrayList<String> ts = new ArrayList<String>();
		ts.add(0, request.getParameter("ts"));
		ts.add(1, session.getId());

		// execute only if an operation is entered by the client
		if (ts.get(0) != null) {

			// handling 'commit' requests for all the algorithms and 'wait and retry'
			// request for FOCC algorithm
			if (ts.get(0).toLowerCase().contains("commit")

					|| ts.get(0).toLowerCase().contains("wait and retry")) {

				try {
					// to handle the case where user forgets to choose an algorithm
					if (currentAlgorithm == null) {
						result.add(0, "<font color=\"red\">Please select an algorithm!</font>");
						nmbrOfCallsToServlet = 0;
					} else {
						result.add(0, commitTransaction(ts.get(0), currentAlgorithm, sessonTranPair.get(ts.get(1))));
					}
				} catch (InterruptedException e) {
					result.add(0, "Synchronization of session has been Interrupted. Please restat the application");
				}
				session = request.getSession(); // has to be called again after the wait() to regain the state of the
												// session of the waiting request.
				nmbrOfCallsToServlet++;

				// this if code makes sure not to display the same message of "Please select an
				// algorithm" more than once. And when user finally realizes and chooses the
				// algorithm, this message is overwritten with the result of entered operation
				if (nmbrOfCallsToServlet == 2 && session.getAttribute("t1")
						.equals("<font color=\"red\">Please select an algorithm!</font>")) {
					nmbrOfCallsToServlet = 1;
				}
				request.setAttribute("counter", nmbrOfCallsToServlet);
				session.setAttribute("t" + Integer.toString(nmbrOfCallsToServlet), result.get(0));

				// for FOCC algorithm, when just providing msg for available options, we do not
				// have to increment the transaction ID, rest of the time we have to increment
				// since the current transaction is committed
				if (!result.get(0).contains("This transaction Conflicts with another active transaction #")
						&& !result.get(0).contains("Please select an algorithm!")) {

					tranIDList.add(tranID);
					unlockStart.put(tranID, false);
					sessonTranPair.replace(ts.get(1), tranID++);
					session.setAttribute("tranID", sessonTranPair.get(ts.get(1)));
					session.removeAttribute("timestamp");
				}

			}

			// handling requests other than 'commit' request
			else {

				try {
					if (currentAlgorithm == null) {
						result.add(0, "<font color=\"red\">Please select an algorithm!</font>");
						nmbrOfCallsToServlet = 0;
					} else {
						result = concurrencyAlgorithm(ts.get(0), currentAlgorithm, sessonTranPair.get(ts.get(1)));
					}
				} catch (InterruptedException e) {
					result.add(0, "Synchronization of session has been Interrupted. Please restat the application");
				}
				session = request.getSession(); // has to be called again after the wait() to regain the state
												// of
												// the session of the waiting request.
				nmbrOfCallsToServlet++;
				if (session.getAttribute("t1") != null) {
					if (nmbrOfCallsToServlet == 2 && session.getAttribute("t1")
							.equals("<font color=\"red\">Please select an algorithm!</font>")) {
						nmbrOfCallsToServlet = 1;
					}
				}

				request.setAttribute("counter", nmbrOfCallsToServlet);
				session.setAttribute("t" + Integer.toString(nmbrOfCallsToServlet), result.get(0));
				session.setAttribute("tranID", sessonTranPair.get(ts.get(1)));

			}
		}

		synchronized (lock) {

			// forward the request to jsp, which then will display the message outputs set
			// into session attributes
			dispatcher.forward(request, response);
			// notify all the suspended threads, so they can resume
			lock.notifyAll();
		}

	}

	/**
	 * Destroy all the saved session attributes saved in the web container.
	 */
	@Override
	public void destroy() {
		Enumeration<String> attributeNames = session.getAttributeNames();
		while (attributeNames.hasMoreElements()) {
			session.removeAttribute(attributeNames.nextElement().toString());
		}
		session.invalidate();
	}

	/**
	 * This method handles the input string (other than "commit" and "wait and retry
	 * validation later") provided by the client. Depending upon the algorithm
	 * selected, the methods from the respective classes are called and executed,
	 * and appropriate output is generated to display.
	 * 
	 * @param ts        - input string by the client
	 * @param algorithm
	 * @param tranID    - current transaction ID
	 * @return The output message that is displayed to the client.
	 * @throws InterruptedException
	 */
	private ArrayList<String> concurrencyAlgorithm(String ts, String algorithm, long tranID)
			throws InterruptedException {

		ArrayList<String> result = null;

		if (algorithm.contains("Two-Phase Locking")) {
			session.removeAttribute("timestamp");
			TwoPhaseLocking_V2 twoPL = new TwoPhaseLocking_V2(tranID);
			result = twoPL.transactionResult(ts);
			if (result.size() > 1) {
				if (result.get(1).contains("abort")) {
					cleanLockTable(lockTable, tranID);
					modifyTempTable(expressionResultStorage2PL, tranID);
					tranIDList.add(this.tranID);
					unlockStart.put(this.tranID, false);
					sessonTranPair.replace(session.getId(), this.tranID++);
				}
			}
		} else if (algorithm.contains("Timestamp Ordering")) {
			if (session.getAttribute("timestamp") == null) {
				session.setAttribute("timestamp", new Date());
				transTimeStamp.put(tranID, System.currentTimeMillis()); // time-stamp for the transaction is set here-
																		// when the first operation is requested
			}

			TimestampOrdering timeStampOrdering = new TimestampOrdering(tranID);
			result = timeStampOrdering.transactionResult(ts);

			if (result.size() > 1) {
				if (result.get(1).contains("abort")) {
					modifyTempTable(expressionResultStorageTO, tranID);
					session.removeAttribute("timestamp");
					tranIDList.add(this.tranID);
					sessonTranPair.replace(session.getId(), this.tranID++);
				}
			}

		} else if (algorithm.contains("Optimistic Concurrency Control BOCC")) {
			OptimisticConcurrencyControlBOCC bocc = new OptimisticConcurrencyControlBOCC(tranID);
			result = bocc.transactionResult(ts);

			if (result.size() > 1) {
				if (result.get(1).contains("aborted")) { // this handling the scenario where client chooses to
															// explicitly abort the
															// transaction
					tranIDList.add(this.tranID);
					sessonTranPair.replace(session.getId(), this.tranID++);
				}
			}

		} else if (algorithm.contains("Optimistic Concurrency Control FOCC")) {
			OptimisticConcurrencyControlFOCC focc = new OptimisticConcurrencyControlFOCC(tranID);
			result = focc.transactionResult(ts);

			if (result.size() > 1) {
				if (result.get(1).contains("aborted")) {
					tranIDList.add(this.tranID);
					sessonTranPair.replace(session.getId(), this.tranID++);
				}
			}

		}
		return result;
	}

	/**
	 * Depending upon the algorithm running, the transaction is either committed or
	 * aborted when this method is called.
	 * 
	 * @param ts        - input string by the client
	 * @param algorithm - the selected algorithm by the client
	 * @param tranID    - current transaction ID
	 * @return The output message that is displayed to the client.
	 * @throws InterruptedException - because FOCC algorithm implements wait()
	 *                              method for current transaction when there is a
	 *                              conflicting transaction running.
	 */
	private String commitTransaction(String ts, String algorithm, long tranID) throws InterruptedException {

		String commitMessage = null;
		if (algorithm.contains("Two-Phase Locking")) {
			cleanLockTable(lockTable, tranID);
			modifyTempTable(expressionResultStorage2PL, tranID);

			commitMessage = "<font color=\"blue\"> <b>committed transaction #" + Long.toString(tranID)
					+ " !</b></font> <br/>";

		} else if (algorithm.contains("Timestamp Ordering")) {
			modifyTempTable(expressionResultStorageTO, tranID);

			commitMessage = "<font color=\"blue\"> <b>committed transaction #" + Long.toString(tranID)
					+ " !</b></font> <br/>";

		} else if (algorithm.contains("Optimistic Concurrency Control BOCC")) {

			OptimisticConcurrencyControlBOCC bocc = new OptimisticConcurrencyControlBOCC(tranID);
			bocc.validateTransaction();
			if (transactionPhaseBOCC.get(tranID) != "validated") {
				commitMessage = "<font color=\"red\"> <b>ABORTED Transaction #" + Long.toString(tranID)
						+ "--> BOCC Validation failed!</b>" + transactionPhaseBOCC.get(tranID) + "</font> <br/>";
			} else if (transactionPhaseBOCC.get(tranID) == "validated") {
				commitMessage = "<font color=\"blue\"> <b>Validated and Committed Transaction #" + Long.toString(tranID)
						+ " ! </b></font><br/>";
			}

			modifyTempTable(expressionResultStorageBOCC, tranID);

		} else if (algorithm.contains("Optimistic Concurrency Control FOCC")) {

			// for option - wait and retry : wait until the conflicting transaction is
			// terminated (committed/aborted/validated)

			if (ts.toLowerCase().contains("wait and retry")) {
				transactionPhaseFOCC.replace(tranID, "read phase"); // reset current transaction to read phase so that
																	// conflicting transaction can enter validation
				synchronized (lock) {
					while (transactionPhaseFOCC.get(conflictingTranID) != "validated"
							&& !transactionPhaseFOCC.get(conflictingTranID).contains("aborted")
							&& transactionPhaseFOCC.get(conflictingTranID) != "committed") {

						lock.wait();
						// after the state of conflicting transaction is changed to either 'validated',
						// 'aborted' or 'committed' the waiting transaction come out of waiting and
						// executes below code again as "Retry"
					}
				}
			}

			OptimisticConcurrencyControlFOCC focc = new OptimisticConcurrencyControlFOCC(tranID);
			String conflictTranID = focc.validateTransaction();
			if (conflictTranID != null) {
				commitMessage = "<br />This transaction Conflicts with another active transaction #" + conflictTranID
						+ "<br /> Depending upon how you want to handle the conflict, Please enter below options into above text-box: <br/>"
						+ " - Abort this transaction<br/> - Abort conflicting transaction #" + conflictTranID
						+ "<br/> - Wait and Retry validation later <br/>";
			} else if (transactionPhaseFOCC.get(tranID) == "validated") {
				// message for successful validation
				commitMessage = "<font color=\"blue\"> <b>Validated and Committed Transaction #" + Long.toString(tranID)
						+ " ! </b></font><br/>";
				modifyTempTable(expressionResultStorageFOCC, tranID);
			} else if (transactionPhaseFOCC.get(conflictingTranID) == "committed") {
				// this message is for read-only transactions
				commitMessage = "<font color=\"blue\"> <b>Committed Transaction #" + Long.toString(tranID)
						+ " ! </b></font><br/>";
				modifyTempTable(expressionResultStorageFOCC, tranID);
			}
		}

		return commitMessage;
	}

	private void modifyTempTable(HashMap<String, Double> tempTable, Long tranID) {
		// Create a Iterator to KeySet of temp table
		Iterator<String> tempKeyset = tempTable.keySet().iterator();

		// Iterate over all the elements to find keys related to committed transaction
		// modify the keys in order to retain the temp values for further operations
		// after commit
		ArrayList<String> newKeys = new ArrayList<String>();
		ArrayList<Double> values = new ArrayList<Double>();
		while (tempKeyset.hasNext()) {
			String key = tempKeyset.next();
			// Check if Value associated with Key is equal to tranID for this commit
			if (key.contains("(" + Long.toString(tranID) + ")")) {
				newKeys.add(key.replace("(" + Long.toString(tranID) + ")", "(" + Long.toString(this.tranID) + ")"));
				values.add(tempTable.get(key));
				// remove if Value associated with Key is equal to tranID of committed
				// transaction
				tempKeyset.remove();
			}
		}

		// saving the temp values associated with current tranID, to make it available
		// for next transaction
		for (int i = 0; i < newKeys.size(); i++) {
			tempTable.put(newKeys.get(i), values.get(i));
		}
	}

	private void cleanLockTable(HashMap<String, Long> lockTable, Long tranID) {
		// Create a Iterator to KeySet of lock table
		Iterator<String> keyset = lockTable.keySet().iterator();

		// Iterate over all the lock table keys to remove locks for committed
		// transaction
		while (keyset.hasNext()) {
			String key = keyset.next(); // java.util.ConcurrentModificationException
			// Check if Value associated with Key is equal to tranID for this commit
			if (lockTable.get(key) == tranID) {
				keyset.remove();
			}
		}
	}

}
