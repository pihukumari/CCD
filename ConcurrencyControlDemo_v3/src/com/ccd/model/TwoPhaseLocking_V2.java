package com.ccd.model;

import java.util.ArrayList;
import java.util.HashMap;

import com.ccd.controler.ControllerServlet_V2;
import com.ccd.model.TransactionStmtTransformation;

public class TwoPhaseLocking_V2 {

	HashMap<String, Double> tempTable = new HashMap<String, Double>();
	long tranID;

	/**
	 * 
	 */
	public TwoPhaseLocking_V2(HashMap<String, Double> tempTable, long tranID) {
		super();
		this.tempTable = tempTable;
		this.tranID = tranID;
	}

	public ArrayList<String> transactionResult(String ts) {

		ArrayList<String> returnString = new ArrayList<String>();

		TransactionStmtTransformation transactionStmtTransformation = new TransactionStmtTransformation();
		String operationType = transactionStmtTransformation.operationType(ts);
		String dataElement = transactionStmtTransformation.dataElement(ts, operationType);

		boolean exclusiveLockbyOtherTransaction = isExclusiveLockbyOtherTransaction(dataElement);
		boolean exclusiveLockbyThisTransaction = isExclusiveLockbyThisTransaction(dataElement);

		switch (operationType) {
		case "read":
			if (ControllerServlet_V2.transTable.containsKey(dataElement)) {

				while (exclusiveLockbyOtherTransaction) {
					exclusiveLockbyOtherTransaction = isExclusiveLockbyOtherTransaction(dataElement);
				}
				if (exclusiveLockbyThisTransaction) {
					returnString.add(0, ts + " --> " + dataElement + " = "
							+ ControllerServlet_V2.transTable.get(dataElement).toString());
				} else {
					ControllerServlet_V2.lockTable.put("S" + dataElement, tranID); // work here
					returnString.add(0, ts + " --> Shared lock for 'Read' --> " + dataElement + " = "
							+ ControllerServlet_V2.transTable.get(dataElement).toString());
				}
			} else {
				ControllerServlet_V2.lockTable.put("S" + dataElement, tranID);
				ControllerServlet_V2.transTable.put(dataElement, 0.0);
				returnString.add(0, ts + " --> Share lock for 'Read' --> " + dataElement + " = "
						+ ControllerServlet_V2.transTable.get(dataElement).toString());
			}
			break;
		case "expression":
			ArrayList<String> expressionSolution = transactionStmtTransformation.solveExpression(ts,
					ControllerServlet_V2.transTable);
			if (expressionSolution.size() == 1) {
				returnString.add(0, ts + " --> " + expressionSolution.get(0));
			} else if (expressionSolution.size() > 1) {
				returnString.add(0, ts + " --> " + expressionSolution.get(0) + " = " + expressionSolution.get(1));
				returnString.add(1, expressionSolution.get(0));
				returnString.add(2, expressionSolution.get(1));
			}
			break;
		case "write":
			if (tempTable.containsKey(dataElement)) {
				returnString.add(0, write(ts, dataElement, tempTable.get(dataElement)));
				tempTable.remove(dataElement);
			} else if (!tempTable.containsKey(dataElement)
					&& ControllerServlet_V2.transTable.containsKey(dataElement)) {
				returnString.add(0, write(ts, dataElement, ControllerServlet_V2.transTable.get(dataElement)));

			} else {
				ControllerServlet_V2.transTable.put(dataElement, 0.0);
				returnString.add(0, write(ts, dataElement, ControllerServlet_V2.transTable.get(dataElement)));
			}
			break;
		}

		return returnString;

	}

	public String write(String ts, String dataElement, double value) {

		String returnString = null;

		boolean exclusiveLockbyOtherTransaction = isExclusiveLockbyOtherTransaction(dataElement);
		boolean sharedLockbyOtherTransaction = isSharedLockbyOtherTransaction(dataElement);
		boolean sharedLockbyThisTransaction = isSharedLockbyThisTransaction(dataElement);
		boolean exclusiveLockbyThisTransaction = isExclusiveLockbyThisTransaction(dataElement);

		while (exclusiveLockbyOtherTransaction || sharedLockbyOtherTransaction) {
			exclusiveLockbyOtherTransaction = isExclusiveLockbyOtherTransaction(dataElement);
			sharedLockbyOtherTransaction = isSharedLockbyOtherTransaction(dataElement);
		}
		if (sharedLockbyThisTransaction) {
			ControllerServlet_V2.lockTable.remove("S" + dataElement, tranID);
			ControllerServlet_V2.lockTable.put("X" + dataElement, tranID);
			ControllerServlet_V2.transTable.replace(dataElement, value);
			returnString = ts + " --> Exclusive lock for 'Update' --> " + dataElement + " = "
					+ ControllerServlet_V2.transTable.get(dataElement).toString();
		} else if (exclusiveLockbyThisTransaction) {
			ControllerServlet_V2.transTable.replace(dataElement, value);
			returnString = ts + " --> " + dataElement + " = "
					+ ControllerServlet_V2.transTable.get(dataElement).toString();
		} else {
			ControllerServlet_V2.lockTable.put("X" + dataElement, tranID);
			ControllerServlet_V2.transTable.replace(dataElement, value);
			returnString = ts + " --> Exclusive lock for 'Update' --> " + dataElement + " = "
					+ ControllerServlet_V2.transTable.get(dataElement).toString();
		}

		return returnString;
	}

	public boolean isSharedLockbyOtherTransaction(String dataElement) {

		if (ControllerServlet_V2.lockTable.containsKey("S" + dataElement)
				&& ControllerServlet_V2.lockTable.get("S" + dataElement) != tranID) {
			return true;
		} else {
			return false;
		}

	}

	public boolean isExclusiveLockbyOtherTransaction(String dataElement) {

		if (ControllerServlet_V2.lockTable.containsKey("X" + dataElement)
				&& ControllerServlet_V2.lockTable.get("X" + dataElement) != tranID) {
			return true;
		} else {
			return false;
		}
	}

	public boolean isExclusiveLockbyThisTransaction(String dataElement) {

		if (ControllerServlet_V2.lockTable.containsKey("X" + dataElement)
				&& ControllerServlet_V2.lockTable.get("X" + dataElement) == tranID) {
			return true;
		} else {
			return false;
		}
	}

	public boolean isSharedLockbyThisTransaction(String dataElement) {

		if (ControllerServlet_V2.lockTable.containsKey("S" + dataElement)
				&& ControllerServlet_V2.lockTable.get("S" + dataElement) == tranID) {
			return true;
		} else {
			return false;
		}
	}

}
