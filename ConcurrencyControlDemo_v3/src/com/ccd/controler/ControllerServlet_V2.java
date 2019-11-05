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
import com.ccd.model.SequenceGenerator;
import com.ccd.model.SequenceGeneratorInterface;

/**
 * Servlet implementation class ControllerServlet
 */
public class ControllerServlet_V2 extends HttpServlet {
	private static final long serialVersionUID = 1L;

	HttpSession session;
	public HashMap<String, Long> sessonTranPair = new HashMap<String, Long>();
	public static HashMap<String, Double> transTable = new HashMap<String, Double>();
	public static HashMap<String, Long> lockTable = new HashMap<String, Long>();
	HashMap<String, Double> tempTable = new HashMap<String, Double>();

	SequenceGeneratorInterface sq = new SequenceGenerator();
	long tranID;
	private int callCounter;

	public void init() {
		tranID = 1;
		callCounter = 0;
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		session = request.getSession();

		String result = null;
		String commit = request.getParameter("commit");
		if (commit != null) {
			lockTable.clear();
			tempTable.clear();
			callCounter++;
			result = "commited transaction #'" + Long.toString(sessonTranPair.get(session.getId())) + "'";
			request.setAttribute("counter", callCounter);
			session.setAttribute("t" + Integer.toString(callCounter), result);
			sessonTranPair.put(session.getId(), tranID++);
			session.setAttribute("tranID", sessonTranPair.get(session.getId()));
		} else {
			String algorithm = request.getParameter("algorithm");
			String ts = (request.getParameter("ts"));
			if (!sessonTranPair.containsKey(session.getId())) {
				sessonTranPair.put(session.getId(), tranID++);
			}

			if (algorithm != null) {
				session.setAttribute("algo", algorithm);
				session.setAttribute("tranID", sessonTranPair.get(session.getId()));
			}
			// extract from session attribute
			String algo = (String) session.getAttribute("algo");

			if (ts != null) {
				callCounter++;
				result = concurrencyAlgorithm(ts, algo, sessonTranPair.get(session.getId()));
				request.setAttribute("counter", callCounter);
				session.setAttribute("t" + Integer.toString(callCounter), result);
			}
		}
		RequestDispatcher dispatcher = request.getRequestDispatcher("ConcurrencyControlDemo_v3.jsp");
		dispatcher.forward(request, response);

	}

	public String concurrencyAlgorithm(String ts, String algorithm, long tranID) {
		ArrayList<String> result = null;
		if (algorithm.contains("Two_Phase_Locking")) {
			TwoPhaseLocking_V2 twoPL = new TwoPhaseLocking_V2(tempTable, tranID);
			result = twoPL.transactionResult(ts);
			if (result.size() > 1) {
				tempTable.put(result.get(1), Double.parseDouble(result.get(2)));
			}
		}
		return result.get(0);
	}

	public void destroy() {
		// LockTable.removeRow(LockTable.getRow(1));
	}

}
