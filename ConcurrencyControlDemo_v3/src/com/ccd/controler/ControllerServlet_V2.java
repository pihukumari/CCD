package com.ccd.controler;

import java.io.IOException;
import java.util.*;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.ccd.model.TwoPhaseLocking_V2;

/**
 * Servlet implementation class ControllerServlet
 */
public class ControllerServlet_V2 extends HttpServlet {
	private static final long serialVersionUID = 1L;

	HttpSession session;
	public HashMap<String, Long> sessonTranPair = new HashMap<String, Long>();
	public static HashMap<String, Double> transTable = new HashMap<String, Double>();
	public static HashMap<String, Long> lockTable = new HashMap<String, Long>();
	public static HashMap<String, Double> rollbackTable = new HashMap<String, Double>();
	public static HashMap<String, Double> tempTable = new HashMap<String, Double>();
	public static ArrayList<Long> tranIDList = new ArrayList<Long>();
	public static HashMap<Long, Boolean> unlockStart = new HashMap<Long, Boolean>();

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

		RequestDispatcher dispatcher = request.getRequestDispatcher("ConcurrencyControlDemo_v3.jsp");
		session = request.getSession();
		String result = null;

		ArrayList<String> commit = new ArrayList<String>();
		commit.add(0, request.getParameter("commit"));
		if (commit.get(0) != null) {
			commit.add(1, session.getId());
				// Create a Iterator to KeySet of lock table
				Iterator<String> keyset = lockTable.keySet().iterator();

				// Iterate over all the lock table keys to remove locks for committed transaction
				while (keyset.hasNext()) {
					String key = keyset.next();
					// Check if Value associated with Key is equal to tranID for this commit
					if (lockTable.get(key) == sessonTranPair.get(commit.get(1))) {
						lockTable.remove(key);
					}
				}
				
				// Create a Iterator to KeySet of temp table 
				Iterator<String> tempKeyset = tempTable.keySet().iterator();

				// Iterate over all the elements to find keys related to committed transaction
				// modify the keys in order to retain the temp values for further operations after commit
				while (tempKeyset.hasNext()) {
					String key = tempKeyset.next();
					// Check if Value associated with Key is equal to tranID for this commit
					if (key.contains(Long.toString(sessonTranPair.get(commit.get(1))))) {
						String newKey = key.replace(Long.toString(sessonTranPair.get(commit.get(1))), Long.toString(tranID));
						tempTable.put(newKey, tempTable.get(key));
						tempTable.remove(key);
					}
				}
				result = "commited transaction #'" + Long.toString(sessonTranPair.get(commit.get(1))) + "'";
				request.setAttribute("counter", nmbrOfCallsToServlet);
				session.setAttribute("t" + Integer.toString(nmbrOfCallsToServlet), result);

				nmbrOfCallsToServlet++;
				tranIDList.add(tranID);
				unlockStart.put(tranID, false);
				sessonTranPair.replace(commit.get(1), tranID++);
				session.setAttribute("tranID", sessonTranPair.get(commit.get(1)));
				synchronized (lock) {
				dispatcher.forward(request, response);
				lock.notifyAll();
			}
		} else {

			ArrayList<String> ts = new ArrayList<String>();
			ts.add(0, request.getParameter("ts"));
			ts.add(1, session.getId());
			String algorithm = request.getParameter("algorithm");

			if (!sessonTranPair.containsKey(session.getId())) {
				tranIDList.add(tranID);
				unlockStart.put(tranID, false);
				sessonTranPair.put(session.getId(), tranID++);
			}

			if (algorithm != null) {
				session.setAttribute("algo", algorithm);
				session.setAttribute("tranID", sessonTranPair.get(session.getId()));
			}
			// extract algorithm name from session attribute "algo"
			String algo = (String) session.getAttribute("algo");

			if (ts.get(0) != null) {
				nmbrOfCallsToServlet++;
				try {
					result = concurrencyAlgorithm(ts.get(0), algo, sessonTranPair.get(ts.get(1)));
					session = request.getSession(); // has to be called again after the wait() to regain the state of
													// the session of the waiting request.
					session.setAttribute("t" + Integer.toString(nmbrOfCallsToServlet), result);
					session.setAttribute("tranID", sessonTranPair.get(ts.get(1)));
					request.setAttribute("counter", nmbrOfCallsToServlet);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			dispatcher.forward(request, response);
		}

	}

	public String concurrencyAlgorithm(String ts, String algorithm, long tranID) throws InterruptedException {
		ArrayList<String> result = null;
		if (algorithm.contains("Two-Phase Locking")) {
			TwoPhaseLocking_V2 twoPL = new TwoPhaseLocking_V2(tranID);
			result = twoPL.transactionResult(ts);
			if (result.size() > 1) {
				tempTable.put(result.get(1) + Long.toString(tranID), Double.parseDouble(result.get(2)));
			}
		}
		return result.get(0);
	}

	public void destroy() {
		// LockTable.removeRow(LockTable.getRow(1));
	}

}
