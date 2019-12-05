package com.ccd.model;

import java.sql.Timestamp;
import java.util.*;

import com.ccd.controler.ControllerServlet_V2;

public class TimestampOrdering {

	long tranID;

	public TimestampOrdering(long tranID) {
		super();
		this.tranID = tranID;
	}

	public ArrayList<String> transactionResult(String ts) {

		ArrayList<String> returnString = new ArrayList<String>();

		TransactionStmtTransformation transactionStmtTransformation = new TransactionStmtTransformation();
		String operationType = transactionStmtTransformation.operationType(ts);
		String dataElement = transactionStmtTransformation.dataElement(ts, operationType);

		boolean shouldAbort = checkBTOrule(operationType, dataElement);
		if (shouldAbort) {
			abort(Long.toString(tranID));
			returnString.add(0, ts + " --> " + "ABORTED Transaction #" + tranID + " ! with timestamp: "
					+ timeStamp(ControllerServlet_V2.transTimeStamp.get(tranID)) + " Not compliant with BTO rule! ");
			returnString.add(1, "abort");
			return returnString;
		}

		switch (operationType) {
		case "read":
			// capture time stamp of each operation in key-value pair where key is TranID +
			// Operation type + data element and value is the current time is millisec

			ControllerServlet_V2.operationTimeStamp.put(Long.toString(tranID) + "read" + dataElement,
					System.currentTimeMillis());

			if (ControllerServlet_V2.transTableTO.containsKey(dataElement)) {
				returnString.add(0,
						ts + " --> " + dataElement + " = "
								+ ControllerServlet_V2.transTableTO.get(dataElement).toString() + " --> Time: "
								+ timeOfOperation());

			} else {
				ControllerServlet_V2.transTableTO.put(dataElement, 0.0);
				returnString.add(0,
						ts + " --> " + dataElement + " = "
								+ ControllerServlet_V2.transTableTO.get(dataElement).toString() + " --> Time: "
								+ timeOfOperation());
			}

			// update read-from relation
			//updateReadFromRelation(dataElement);

			break;
		case "expression":
			ArrayList<String> expressionSolution = transactionStmtTransformation.solveExpression(ts,
					ControllerServlet_V2.transTableTO);
			if (expressionSolution.size() == 1) {
				returnString.add(0, ts + " --> " + expressionSolution.get(0));
			} else if (expressionSolution.size() > 1) {
				returnString.add(0, ts + " --> " + expressionSolution.get(0) + " = " + expressionSolution.get(1));
				returnString.add(1, expressionSolution.get(0));
				returnString.add(2, expressionSolution.get(1));
			}
			break;
		case "write":
			// capture time stamp of each operation in key-value pair where key is TranID +
			// Operation type + data element and value is the current time is millisec
			ControllerServlet_V2.operationTimeStamp.put(Long.toString(tranID) + "write" + dataElement,
					System.currentTimeMillis());

			// Add the data item to Transaction table if it does not have the data item for
			// write operation
			if (!ControllerServlet_V2.transTableTO.containsKey(dataElement)) {
				ControllerServlet_V2.transTableTO.put(dataElement, 0.0);
			}

			// capture old value of data element from transTable
			String oldValue = ""; // initialized to avoid null
			String newValue = "";
			if (ControllerServlet_V2.rollbackTableTO.containsKey(dataElement + "(" + Long.toString(tranID) + ")")) {
				oldValue = ControllerServlet_V2.rollbackTableTO.get(dataElement + "(" + Long.toString(tranID) + ")")
						.split("|")[0];
			} else {
				oldValue = ControllerServlet_V2.transTableTO.get(dataElement).toString();
			}

			if (ControllerServlet_V2.expressionResultStorageTO.containsKey(dataElement + "(" + Long.toString(tranID) + ")")) {

				newValue = ControllerServlet_V2.expressionResultStorageTO.get(dataElement + "(" + Long.toString(tranID) + ")")
						.toString();

				returnString.add(0, write(ts, dataElement,
						ControllerServlet_V2.expressionResultStorageTO.get(dataElement + "(" + Long.toString(tranID) + ")")));
				ControllerServlet_V2.expressionResultStorageTO.remove(dataElement + "(" + Long.toString(tranID) + ")");
			} else {
				newValue = ControllerServlet_V2.transTableTO.get(dataElement).toString();
				returnString.add(0, write(ts, dataElement, ControllerServlet_V2.transTableTO.get(dataElement)));
			}

			// add the before after state of the write operation, so the operations can be
			// undone when the transaction is aborted
			ControllerServlet_V2.rollbackTableTO.put(dataElement + "(" + Long.toString(tranID) + ")",
					oldValue + "|" + newValue);

			// update final-write table
			//ControllerServlet_V2.finalwriteTableTO.put(dataElement, tranID);

			break;
		}

		return returnString;
	}

	private String write(String ts, String dataElement, double value) {

		String returnString = null;

		ControllerServlet_V2.transTableTO.replace(dataElement, value);
		returnString = ts + " --> " + dataElement + " = "
				+ ControllerServlet_V2.transTableTO.get(dataElement).toString() + " --> Time: " + timeOfOperation();
		return returnString;

	}

	private Timestamp timeOfOperation() {
		Timestamp time = new Timestamp(System.currentTimeMillis());
		return time;
	}

	private Timestamp timeStamp(long milliseconds) {
		Timestamp time = new Timestamp(milliseconds);
		return time;
	}

	private boolean checkBTOrule(String operationType, String dataElement) {

		boolean shouldAbort = false;
		if (operationType == "read") {
			for (long i : ControllerServlet_V2.tranIDList) {
				if (i != tranID) {
					String keyToCompare = Long.toString(i) + "write" + dataElement;
					if (ControllerServlet_V2.operationTimeStamp.containsKey(keyToCompare)) {
						if (ControllerServlet_V2.transTimeStamp.get(i) > ControllerServlet_V2.transTimeStamp
								.get(tranID)) {
							shouldAbort = true;
							break;
						}

					}
				}
			}
		} else if (operationType == "write") {
			for (long i : ControllerServlet_V2.tranIDList) {
				if (i != tranID) {
					String readKeyToCompare = Long.toString(i) + "read" + dataElement;
					String writeKeyToCompare = Long.toString(i) + "write" + dataElement;
					if (ControllerServlet_V2.operationTimeStamp.containsKey(readKeyToCompare)) {
						if (ControllerServlet_V2.transTimeStamp.get(i) > ControllerServlet_V2.transTimeStamp
								.get(tranID)) {
							shouldAbort = true;
							break;
						}
					} else if (ControllerServlet_V2.operationTimeStamp.containsKey(writeKeyToCompare)) {
						if (ControllerServlet_V2.transTimeStamp.get(i) > ControllerServlet_V2.transTimeStamp
								.get(tranID)) {
							shouldAbort = true;
							break;
						}
					}
				}
			}
		}
		return shouldAbort;

	}


	public void abort(String tranID) {
		// ** undo 'write' modifications by current tranID since it is aborted
		Iterator<String> keySet = ControllerServlet_V2.rollbackTableTO.keySet().iterator();

		while (keySet.hasNext()) {
			String key = keySet.next();
			if (key.contains("(" + tranID + ")")) {

				double oldVal = Double.parseDouble(ControllerServlet_V2.rollbackTableTO.get(key).split("|")[0]);
				// double newVal =
				// Double.parseDouble(ControllerServlet_V2.rollbackTableTO.get(key).split("|")[1]);
				String dataElement = key.replace("(" + tranID + ")", "");
				ControllerServlet_V2.transTableTO.replace(dataElement, oldVal);
				// ControllerServlet_V2.transTableTO.replace(dataElement, newVal, oldVal);
			}
		}
	}
}
