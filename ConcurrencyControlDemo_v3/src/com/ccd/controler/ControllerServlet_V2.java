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
	public static HashMap<String, Long> lockTable2PL = new HashMap<>();
	public static HashMap<String, Double> expressionResultStorage2PL = new HashMap<>();
	public static HashMap<Long, Boolean> unlockStart = new HashMap<>();

	// **************** Time-stamp Ordering **************//
	public static HashMap<Long, Long> transTimeStamp = new HashMap<>();
	public static HashMap<String, Double> transTableTO = new HashMap<>();
	public static HashMap<String, Double> expressionResultStorageTO = new HashMap<>();
	public static LinkedHashMap<String, String> rollbackTableTO = new LinkedHashMap<>();
	public static LinkedHashMap<String, Long> operationTimeStamp = new LinkedHashMap<>();

	// ********************* BOCC ***************************//
	public static HashMap<String, Long> transLifeTime = new HashMap<>();
	public static HashMap<String, Double> transTableBOCC = new HashMap<>();
	public static HashMap<String, Double> expressionResultStorageBOCC = new HashMap<>();
	public static HashMap<String, Double> privateWorkspaceBOCC = new HashMap<>();
	public static LinkedHashMap<String, String> rollbackTableBOCC = new LinkedHashMap<>();
	public static LinkedHashMap<String, Long> readSetBOCC = new LinkedHashMap<>();
	public static LinkedHashMap<String, Long> writeSetBOCC = new LinkedHashMap<>();
	public static LinkedHashMap<Long, String> transactionPhaseBOCC = new LinkedHashMap<>();

	// ********************* FOCC ***************************//
	public static HashMap<String, Double> transTableFOCC = new HashMap<>();
	public static HashMap<String, Double> expressionResultStorageFOCC = new HashMap<>();
	public static HashMap<String, Double> privateWorkspaceFOCC = new HashMap<>();
	public static LinkedHashMap<String, String> rollbackTableFOCC = new LinkedHashMap<>();
	public static LinkedHashMap<String, Long> readSetFOCC = new LinkedHashMap<>();
	public static LinkedHashMap<String, Long> writeSetFOCC = new LinkedHashMap<>();
	public static LinkedHashMap<Long, String> transactionPhaseFOCC = new LinkedHashMap<>();
	public static long conflictingTranID;

	public static Object lock = new Object();

	long tranID;
	private int nmbrOfCallsToServlet;

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

		request.setAttribute("counter", nmbrOfCallsToServlet);

		RequestDispatcher dispatcher = request.getRequestDispatcher("ConcurrencyControlDemo_v3.jsp");
		session = request.getSession();
		ArrayList<String> result = new ArrayList<>();

		String algorithm = request.getParameter("algorithm");

		if (!sessonTranPair.containsKey(session.getId())) {
			tranIDList.add(tranID);
			sessonTranPair.put(session.getId(), tranID++);
		}

		if (algorithm != null) {
			session.setAttribute("algo", algorithm);
			unlockStart.put(sessonTranPair.get(session.getId()), false);
			session.setAttribute("tranID", sessonTranPair.get(session.getId()));
		}
		// extract algorithm name from session attribute "algo"
		String currentAlgorithm = (String) session.getAttribute("algo");

		ArrayList<String> ts = new ArrayList<String>();
		ts.add(0, request.getParameter("ts"));
		ts.add(1, session.getId());

		if (ts.get(0) != null) {

			// For FOCC algorithm, check if this transaction has been aborted by some other
			// transaction
			// if yes, send out the abort msg and stop the transaction
			if (transactionPhaseFOCC.containsKey(sessonTranPair.get(ts.get(1)))
					&& transactionPhaseFOCC.get(sessonTranPair.get(ts.get(1))).contains("aborted by transaction#")) {

				result.add(0, "This transaction# " + Long.toString(sessonTranPair.get(ts.get(1))) + " has been "
						+ ControllerServlet_V2.transactionPhaseFOCC.get(sessonTranPair.get(ts.get(1))));

				nmbrOfCallsToServlet++;
				request.setAttribute("counter", nmbrOfCallsToServlet);
				session.setAttribute("t" + Integer.toString(nmbrOfCallsToServlet), result.get(0));
				tranIDList.add(tranID);
				unlockStart.put(tranID, false);
				sessonTranPair.replace(ts.get(1), tranID++);
				session.setAttribute("tranID", sessonTranPair.get(ts.get(1)));

			}

			// handling 'commit' requests for all the algorithms and 'wait and retry'
			// request for FOCC algorithm
			else if (ts.get(0).toLowerCase().contains("commit")

					|| ts.get(0).toLowerCase().contains("wait and retry")) {

				try {
					result.add(0, commitTransaction(ts.get(0), currentAlgorithm, sessonTranPair.get(ts.get(1))));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				session = request.getSession(); // has to be called again after the wait() to regain the state of the
												// session of the waiting request.
				nmbrOfCallsToServlet++;
				request.setAttribute("counter", nmbrOfCallsToServlet);
				session.setAttribute("t" + Integer.toString(nmbrOfCallsToServlet), result.get(0));

				// for FOCC algorithm, when just providing msg for available options, we do not
				// have to increment the transaction ID, rest of the time we have to increment
				// since the current transaction is committed
				if (!result.get(0).contains("This transaction Conflicts with another active transaction #")) {

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
					result = concurrencyAlgorithm(ts.get(0), currentAlgorithm, sessonTranPair.get(ts.get(1)));

					session = request.getSession(); // has to be called again after the wait() to regain the state
													// of
													// the session of the waiting request.
					nmbrOfCallsToServlet++;
					request.setAttribute("counter", nmbrOfCallsToServlet);
					session.setAttribute("t" + Integer.toString(nmbrOfCallsToServlet), result.get(0));
					session.setAttribute("tranID", sessonTranPair.get(ts.get(1)));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		}

		synchronized (lock) {
			dispatcher.forward(request, response);
			lock.notifyAll();
		}
		// dispatcher.forward(request, response);

	}

	@Override
	public void destroy() {
		synchronized (lock) {
			lock.notifyAll();
		}
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
	public ArrayList<String> concurrencyAlgorithm(String ts, String algorithm, long tranID)
			throws InterruptedException {
		ArrayList<String> result = null;
		if (algorithm.contains("Two-Phase Locking")) {
			session.removeAttribute("timestamp");
			TwoPhaseLocking_V2 twoPL = new TwoPhaseLocking_V2(tranID);
			result = twoPL.transactionResult(ts);
			if (result.size() > 1) {
				// store values (calculated from expressions) to be used in write operations
				// later
				expressionResultStorage2PL.put(result.get(1) + "(" + Long.toString(tranID) + ")",
						Double.parseDouble(result.get(2)));
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
					session.removeAttribute("timestamp");
					tranIDList.add(this.tranID);
					sessonTranPair.replace(session.getId(), this.tranID++);
				} else {
					// store temp values to be used in write operations later
					expressionResultStorageTO.put(result.get(1) + "(" + Long.toString(tranID) + ")",
							Double.parseDouble(result.get(2)));
				}
			}

		} else if (algorithm.contains("Optimistic Concurrency Control BOCC")) {
			OptimisticConcurrencyControlBOCC bocc = new OptimisticConcurrencyControlBOCC(tranID);
			result = bocc.transactionResult(ts);

			if (result.size() > 1) {
				if (result.get(1).contains("aborted")) { // this handling the scenario where client chooses to abort the
															// transaction
					tranIDList.add(this.tranID);
					sessonTranPair.replace(session.getId(), this.tranID++);
				} else {
					// store temp values to be used in write operations later
					expressionResultStorageBOCC.put(result.get(1) + "(" + Long.toString(tranID) + ")",
							Double.parseDouble(result.get(2)));
				}
			}

		} else if (algorithm.contains("Optimistic Concurrency Control FOCC")) {
			OptimisticConcurrencyControlFOCC focc = new OptimisticConcurrencyControlFOCC(tranID);
			result = focc.transactionResult(ts);

			if (result.size() > 1) {
				if (result.get(1).contains("aborted")) {
					tranIDList.add(this.tranID);
					sessonTranPair.replace(session.getId(), this.tranID++);
				} else {
					// store temp values to be used in write operations later
					expressionResultStorageFOCC.put(result.get(1) + "(" + Long.toString(tranID) + ")",
							Double.parseDouble(result.get(2)));
				}
			}

		} else if (algorithm.contains("Snapshot Isolation")) {

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
			// Create a Iterator to KeySet of lock table
			Iterator<String> keyset = lockTable2PL.keySet().iterator();

			// Iterate over all the lock table keys to remove locks for committed
			// transaction
			while (keyset.hasNext()) {
				String key = keyset.next(); // java.util.ConcurrentModificationException
				// Check if Value associated with Key is equal to tranID for this commit
				if (lockTable2PL.get(key) == tranID) {
					keyset.remove();
				}
			}

			// Create a Iterator to KeySet of temp table
			Iterator<String> tempKeyset = expressionResultStorage2PL.keySet().iterator();

			// Iterate over all the elements to find keys related to committed transaction
			// modify the keys in order to retain the temp values for further operations
			// after commit
			while (tempKeyset.hasNext()) {
				String key = tempKeyset.next();
				// Check if Value associated with Key is equal to tranID for this commit
				if (key.contains("(" + Long.toString(tranID) + ")")) {
					String newKey = key.replace("(" + Long.toString(tranID) + ")",
							"(" + Long.toString(this.tranID) + ")");
					expressionResultStorage2PL.put(newKey, expressionResultStorage2PL.get(key));
					tempKeyset.remove();
				}
			}

			commitMessage = "committed transaction #" + Long.toString(tranID)
					+ " ! <br/> ******************************************* <br/>";

		} else if (algorithm.contains("Timestamp Ordering")) {
			// Create a Iterator to KeySet of rollback table
			Iterator<String> keyset = rollbackTableTO.keySet().iterator();

			// Iterate over all the elements to find operations related to committed
			// transaction
			// remove them after commit
			while (keyset.hasNext()) {
				String key = keyset.next();
				// Check if key is associated with tranID for this commit,if yes- remove
				if (key.contains("(" + Long.toString(tranID) + ")")) {
					keyset.remove();
				}
			}

			// Create a Iterator to KeySet of temp table
			Iterator<String> tempKeyset = expressionResultStorageTO.keySet().iterator();

			// Iterate over all the elements to find keys related to committed transaction
			// modify the keys in order to retain the temp values for further operations
			// after commit
			while (tempKeyset.hasNext()) {
				String key = tempKeyset.next();
				// Check if Value associated with Key is equal to tranID for this commit
				if (key.contains("(" + Long.toString(tranID) + ")")) {
					String newKey = key.replace("(" + Long.toString(tranID) + ")",
							"(" + Long.toString(this.tranID) + ")");
					expressionResultStorageTO.put(newKey, expressionResultStorageTO.get(key));
					tempKeyset.remove();
				}
			}

			commitMessage = "committed transaction #" + Long.toString(tranID)
					+ " ! <br/> ******************************************* <br/>";

		} else if (algorithm.contains("Optimistic Concurrency Control BOCC")) {

			OptimisticConcurrencyControlBOCC bocc = new OptimisticConcurrencyControlBOCC(tranID);
			bocc.validateTransaction();
			if (transactionPhaseBOCC.get(tranID) != "validated") {
				commitMessage = "ABORTED Transaction #" + Long.toString(tranID) + "--> BOCC Validation failed!"
						+ transactionPhaseBOCC.get(tranID) + " <br/> ******************************************* <br/>";
			} else if (transactionPhaseBOCC.get(tranID) == "validated") {
				commitMessage = "Validated and Committed Transaction #" + Long.toString(tranID) + " ! <br/> ******************************************* <br/>";
			}

			// Create a Iterator to KeySet of temp table
			Iterator<String> tempKeyset = expressionResultStorageBOCC.keySet().iterator();

			// Iterate over all the elements to find keys related to committed transaction
			// modify the keys in order to retain the temp values for further operations
			// after commit
			while (tempKeyset.hasNext()) {
				String key = tempKeyset.next();
				// Check if Value associated with Key is equal to tranID for this commit
				if (key.contains("(" + Long.toString(tranID) + ")")) {
					String newKey = key.replace("(" + Long.toString(tranID) + ")",
							"(" + Long.toString(this.tranID) + ")");
					expressionResultStorageBOCC.put(newKey, expressionResultStorageBOCC.get(key));
					tempKeyset.remove();
				}
			}
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
			} else if (transactionPhaseFOCC.get(conflictingTranID) == "validated") {
				// message for successful validation
				commitMessage = "Validated and Committed Transaction #" + Long.toString(tranID) + " ! <br/> ******************************************* <br/>";
			} else if (transactionPhaseFOCC.get(conflictingTranID) == "committed") {
				// this message is for read-only transactions
				commitMessage = "Committed Transaction #" + Long.toString(tranID) + " ! <br/> ******************************************* <br/>";
			}
		}

		return commitMessage;
	}

}
