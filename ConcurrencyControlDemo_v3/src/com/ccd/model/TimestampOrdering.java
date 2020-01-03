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

		// check if current transaction has dirty reads. If yes, abort it
		if (operationType != "abort") {
			abortIfReadingFromAbortedTransaction();
		}

		// if it is aborted in the above statement, send output message to servlet. Else
		// continue normal execution
		if (ControllerServlet_V2.abortedTransactionsListTO.containsKey(tranID)) {
			returnString.add(0, "<font color=\"red\">This transaction #" + Long.toString(tranID) + " has been "
					+ ControllerServlet_V2.abortedTransactionsListTO.get(tranID) + "!</font>");
			returnString.add(1, "abort");
			return returnString;
		}

		// Check the currently requested operation of current transaction against BTO
		// rule. If it fails, it's aborted.
		try {
			boolean shouldAbort = checkBTOrule(operationType, dataElement);

			if (shouldAbort) {
				abort(Long.toString(tranID));
				returnString.add(0,
						ts + " --> " + "<font color=\"red\"> <b>ABORTED Transaction #" + tranID
								+ " !</b> with timestamp: " + timeStamp(ControllerServlet_V2.transTimeStamp.get(tranID))
								+ " Not compliant with BTO rule! </font><br/>");
				returnString.add(1, "abort");
				return returnString;
			}
		} catch (NullPointerException e) {
			returnString.add(0,"Something went wrong! Please clear the cache and restart the execution!");
			return returnString;
		}

		// else continue the execution of the requested operation

		// add data element to the transTable if it doesn't already exists
		ControllerServlet_V2.transTableTO.putIfAbsent(dataElement, 0.0);

		switch (operationType) {
		case "read":

			// capture time stamp of each operation in key-value pair where key is TranID +
			// Operation type + data element and value is the current time is millisec
			ControllerServlet_V2.operationTimeStamp.put(Long.toString(tranID) + "read" + dataElement,
					System.currentTimeMillis());

			// read the data from transTable and prepare the output message to servlet
			returnString.add(0,
					ts + " --> " + dataElement + " = " + ControllerServlet_V2.transTableTO.get(dataElement).toString()
							+ " --> Time: " + timeOfOperation());

			// update readfromRelation table with information about from where this
			// transaction is reading the data -> from its own write or from write of
			// another transaction
			if (ControllerServlet_V2.finalWriteTO.containsKey(dataElement)) {
				ControllerServlet_V2.readFromRelationTO.put(dataElement + "(" + Long.toString(tranID) + ")",
						ControllerServlet_V2.finalWriteTO.get(dataElement));
			}

			break;

		case "expression":
			ArrayList<String> expressionSolution = transactionStmtTransformation.solveExpression(ts,
					ControllerServlet_V2.transTableTO);

			if (expressionSolution.size() == 1) {
				// to handle the message about missing data item
				returnString.add(0, ts + " --> " + expressionSolution.get(0));

			} else if (expressionSolution.size() > 1) {
				// prepare the output message to servlet
				returnString.add(0, ts + " --> " + expressionSolution.get(0) + " = " + expressionSolution.get(1));

				// store values (calculated from expressions) to be used in write operations
				// later
				ControllerServlet_V2.expressionResultStorageTO.put(
						expressionSolution.get(0) + "(" + Long.toString(tranID) + ")",
						Double.parseDouble(expressionSolution.get(1)));
			}
			break;

		case "write":
			// capture time stamp of each operation in key-value pair where key is TranID +
			// Operation type + data element and value is the current time is millisec
			ControllerServlet_V2.operationTimeStamp.put(Long.toString(tranID) + "write" + dataElement,
					System.currentTimeMillis());

			// add the before state of the write operation, so the operations can be
			// undone when the transaction is aborted
			ControllerServlet_V2.rollbackTableTO.put(dataElement + "(" + Long.toString(tranID) + ")",
					ControllerServlet_V2.transTableTO.get(dataElement));

			// check if user has assigned a value to data item using an expression
			if (ControllerServlet_V2.expressionResultStorageTO
					.containsKey(dataElement + "(" + Long.toString(tranID) + ")")) {
				// if yes, use it
				returnString.add(0, write(ts, dataElement, ControllerServlet_V2.expressionResultStorageTO
						.get(dataElement + "(" + Long.toString(tranID) + ")")));
				ControllerServlet_V2.expressionResultStorageTO.remove(dataElement + "(" + Long.toString(tranID) + ")");

			} else {
				// else rewrite the same value from transTable
				returnString.add(0, write(ts, dataElement, ControllerServlet_V2.transTableTO.get(dataElement)));
			}

			ControllerServlet_V2.finalWriteTO.put(dataElement, tranID);

			break;

		// handle the explicit request to abort the transaction
		case "abort":
			abort(Long.toString(tranID));
			returnString.add(0, ts + " --> <font color=\"red\"> <b>ABORTED transaction #" + Long.toString(tranID)
					+ " !</b></font> <br/>");
			returnString.add(1, "abort");
			break;

		}

		return returnString;
	}

	/**
	 * Write the value of data item into transTable
	 * 
	 * @param ts
	 * @param dataElement
	 * @param value
	 * @return
	 */
	private String write(String ts, String dataElement, double value) {

		String returnString = null;

		ControllerServlet_V2.transTableTO.replace(dataElement, value);
		returnString = ts + " --> " + dataElement + " = "
				+ ControllerServlet_V2.transTableTO.get(dataElement).toString() + " --> Time: " + timeOfOperation();
		return returnString;

	}

	/**
	 * Generates timestamp in format yyyy-mm-dd hh:mm:ss.fff
	 * 
	 * @return
	 */
	private Timestamp timeOfOperation() {
		Timestamp time = new Timestamp(System.currentTimeMillis());
		return time;
	}

	/**
	 * Converts millisecond into format yyyy-mm-dd hh:mm:ss.fff
	 * 
	 * @param milliseconds
	 * @return
	 */
	private Timestamp timeStamp(long milliseconds) {
		Timestamp time = new Timestamp(milliseconds);
		return time;
	}

	/**
	 * Checks the operation of a transaction against BTO rule.
	 * 
	 * @param operationType
	 * @param dataElement
	 * @return Returns true if transaction fails BTO check, else returns false.
	 */
	private boolean checkBTOrule(String operationType, String dataElement) throws NullPointerException {

		boolean shouldAbort = false;

		if (operationType == "read") {
			for (long i : ControllerServlet_V2.tranIDList) {
				if (i != tranID) {
					String keyToCompare = Long.toString(i) + "write" + dataElement;
					if (ControllerServlet_V2.operationTimeStamp.containsKey(keyToCompare)) {
						if (ControllerServlet_V2.transTimeStamp.get(tranID) < ControllerServlet_V2.transTimeStamp
								.get(i)) {
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
						if (ControllerServlet_V2.transTimeStamp.get(tranID) < ControllerServlet_V2.transTimeStamp
								.get(i)) {
							shouldAbort = true;
							break;
						}
					} else if (ControllerServlet_V2.operationTimeStamp.containsKey(writeKeyToCompare)) {
						if (ControllerServlet_V2.transTimeStamp.get(tranID) < ControllerServlet_V2.transTimeStamp
								.get(i)) {
							shouldAbort = true;
							break;
						}
					}
				}
			}
		}
		return shouldAbort;

	}

	/**
	 * It is called when abort request is executed, or when current transaction
	 * fails BTO rule. It reverses the effects of writes and removes final write set
	 * of the aborting transaction. And adds it to the list of aborted transactions,
	 * so that application can detect dirty reads for other transactions and hence
	 * can abort them too.
	 * 
	 * @param tranID
	 */
	public void abort(String tranID) {
		// ** undo 'write' modifications by current tranID since it is aborted
		Iterator<String> keySet = ControllerServlet_V2.rollbackTableTO.keySet().iterator();

		while (keySet.hasNext()) {
			String key = keySet.next();
			String dataElement = null;
			if (key.contains("(" + tranID + ")")) {

				double oldVal = ControllerServlet_V2.rollbackTableTO.get(key);
				dataElement = key.replace("(" + tranID + ")", "");
				ControllerServlet_V2.transTableTO.replace(dataElement, oldVal);
			}

			// remove the next entries for the same data item from other transactions that
			// wrote its value after this aborting transaction. It is to prevent the effects
			// of cascade abort from reversing to the false value and cause data
			// inconsistency
			if (dataElement != null && key.contains(dataElement)) {
				keySet.remove();
			}
		}

		// remove the final write set of the current transaction
		Iterator<String> finalWritekeySet = ControllerServlet_V2.finalWriteTO.keySet().iterator();

		while (keySet.hasNext()) {
			String key = keySet.next();
			if (ControllerServlet_V2.finalWriteTO.get(key) == Long.parseLong(tranID)) {
				finalWritekeySet.remove();
			}
		}

		// add the current transaction to aborted list.
		ControllerServlet_V2.abortedTransactionsListTO.putIfAbsent(Long.parseLong(tranID), "aborted");
	}

	/**
	 * This method is called to cause cascade abort in case of a dirty read.
	 */
	private void abortIfReadingFromAbortedTransaction() {
		Iterator<String> readRelationInterator = ControllerServlet_V2.readFromRelationTO.keySet().iterator();

		while (readRelationInterator.hasNext()) {
			String key = readRelationInterator.next();
			if (key.contains("(" + tranID + ")")) {

				long readingFromTranID = ControllerServlet_V2.readFromRelationTO.get(key);

				if (ControllerServlet_V2.abortedTransactionsListTO.containsKey(readingFromTranID)) {
					ControllerServlet_V2.abortedTransactionsListTO.putIfAbsent(tranID,
							"aborted because it is reading from an aborted transaction #"
									+ Long.toString(readingFromTranID));
					abort(Long.toString(tranID));
				}
			}
		}
	}
}
