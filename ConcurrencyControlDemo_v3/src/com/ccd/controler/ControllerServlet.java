package com.ccd.controler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.ccd.model.TwoPhaseLocking;

/**
 * Servlet implementation class ControllerServlet
 */
public class ControllerServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	String dbFile = "C:\\Users\\Kumari\\Documents\\University\\Study\\Master Thesis\\ccd_transaction.xlsx";
	FileInputStream inputStream;
	Workbook workbook;
	XSSFSheet TransTable;
	XSSFSheet LockTable;
	HashMap<String, Double> transHashMap = new HashMap<String, Double>();

	public void init() {
		System.out.println("Initialized");
		try {
			inputStream = new FileInputStream(new File(dbFile));
			workbook = new XSSFWorkbook(inputStream);
			System.out.println("worbook set");
			// Get desired sheets from the workbook
			TransTable = (XSSFSheet) workbook.getSheetAt(0);
			LockTable = (XSSFSheet) workbook.getSheetAt(1);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		HttpSession session = request.getSession();
		ArrayList<String> result = null;

		String algorithm = request.getParameter("algorithm");
		String ts1 = (request.getParameter("ts1"));
		String ts2 = (request.getParameter("ts2"));
		String ts3 = (request.getParameter("ts3"));
		String ts4 = (request.getParameter("ts4"));
		String ts5 = (request.getParameter("ts5"));
		String ts6 = (request.getParameter("ts6"));
		String ts7 = (request.getParameter("ts7"));
		String ts8 = (request.getParameter("ts8"));

		if (algorithm != null) {
			session.setAttribute("algo", algorithm);
		}
		// extract from session attribute
		String algo = (String) session.getAttribute("algo");

		if (ts1 != null) {
			if (algo.contains("Two_Phase_Locking")) {
				TwoPhaseLocking twoPL = new TwoPhaseLocking(TransTable, LockTable, transHashMap);
				result = twoPL.transactionResult(ts1);
				if (result.size() > 1) {
					transHashMap.put(result.get(1), Double.parseDouble(result.get(2)));
				}
			}
			session.setAttribute("t1", result.get(0));
		}
		if (ts2 != null) {
			if (algo.contains("Two_Phase_Locking")) {
				TwoPhaseLocking twoPL = new TwoPhaseLocking(TransTable, LockTable, transHashMap);
				result = twoPL.transactionResult(ts2);
				if (result.size() > 1) {
					transHashMap.put(result.get(1), Double.parseDouble(result.get(2)));
				}
			}
			session.setAttribute("t2", result.get(0));
		}
		if (ts3 != null) {
			if (algo.contains("Two_Phase_Locking")) {
				TwoPhaseLocking twoPL = new TwoPhaseLocking(TransTable, LockTable, transHashMap);
				result = twoPL.transactionResult(ts3);
				if (result.size() > 1) {
					transHashMap.put(result.get(1), Double.parseDouble(result.get(2)));
				}
			}
			session.setAttribute("t3", result.get(0));
		}
		if (ts4 != null) {
			if (algo.contains("Two_Phase_Locking")) {
				TwoPhaseLocking twoPL = new TwoPhaseLocking(TransTable, LockTable, transHashMap);
				result = twoPL.transactionResult(ts4);
				if (result.size() > 1) {
					transHashMap.put(result.get(1), Double.parseDouble(result.get(2)));
				}
			}
			session.setAttribute("t4", result.get(0));
		}
		if (ts5 != null) {
			if (algo.contains("Two_Phase_Locking")) {
				TwoPhaseLocking twoPL = new TwoPhaseLocking(TransTable, LockTable, transHashMap);
				result = twoPL.transactionResult(ts5);
				if (result.size() > 1) {
					transHashMap.put(result.get(1), Double.parseDouble(result.get(2)));
				}
			}
			session.setAttribute("t5", result.get(0));
		}
		if (ts6 != null) {
			if (algo.contains("Two_Phase_Locking")) {
				TwoPhaseLocking twoPL = new TwoPhaseLocking(TransTable, LockTable, transHashMap);
				result = twoPL.transactionResult(ts6);
				if (result.size() > 1) {
					transHashMap.put(result.get(1), Double.parseDouble(result.get(2)));
				}
			}
			session.setAttribute("t6", result.get(0));
		}
		if (ts7 != null) {
			if (algo.contains("Two_Phase_Locking")) {
				TwoPhaseLocking twoPL = new TwoPhaseLocking(TransTable, LockTable, transHashMap);
				result = twoPL.transactionResult(ts7);
				if (result.size() > 1) {
					transHashMap.put(result.get(1), Double.parseDouble(result.get(2)));
				}
			}
			session.setAttribute("t7", result.get(0));
		}
		if (ts8 != null) {
			if (algo.contains("Two_Phase_Locking")) {
				TwoPhaseLocking twoPL = new TwoPhaseLocking(TransTable, LockTable, transHashMap);
				result = twoPL.transactionResult(ts8);
				if (result.size() > 1) {
					transHashMap.put(result.get(1), Double.parseDouble(result.get(2)));
				}
			}
			session.setAttribute("t8", result.get(0));
		}

		RequestDispatcher dispatcher = request.getRequestDispatcher("ConcurrencyControlDemo_v2.jsp");
		dispatcher.forward(request, response);

	}

	public void destroy() {
		//LockTable.removeRow(LockTable.getRow(1));
	}

}
