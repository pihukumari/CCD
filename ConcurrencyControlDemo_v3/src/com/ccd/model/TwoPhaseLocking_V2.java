package com.ccd.model;

import java.util.ArrayList;
import java.util.Iterator;

import com.ccd.controler.ControllerServlet_V2;
import com.ccd.model.TransactionStmtTransformation;

public class TwoPhaseLocking_V2 {

	long tranID;

	/**
	 * 
	 */
	public TwoPhaseLocking_V2(long tranID) {
		super();
		this.tranID = tranID;
	}

	/**
	 * Its the main method of 2PL algorithm
	 * 
	 * @param ts - the input string entered by client
	 * @return
	 * @throws InterruptedException
	 */
	public ArrayList<String> transactionResult(String ts) throws InterruptedException {

		ArrayList<String> returnString = new ArrayList<String>();

		TransactionStmtTransformation transactionStmtTransformation = new TransactionStmtTransformation();
		String operationType = transactionStmtTransformation.operationType(ts);
		String dataElement = transactionStmtTransformation.dataElement(ts, operationType);

		ControllerServlet_V2.readOnlyTransaction2PL.putIfAbsent(tranID, true);
		if (operationType == "write") {
			ControllerServlet_V2.readOnlyTransaction2PL.replace(tranID, true, false);
		}

		abortIfReadingFromAbortedTransaction();
		if (ControllerServlet_V2.abortedTransactions2PL.containsKey(tranID)) {
			returnString.add(0, "<font color=\"red\">This transaction #" + Long.toString(tranID) + " has been "
					+ ControllerServlet_V2.abortedTransactions2PL.get(tranID) + "!</font>");
			returnString.add(1, "abort");
			return returnString;
		}

		switch (operationType) {
		case "read":

			if (!ControllerServlet_V2.transTable2PL.containsKey(dataElement)) {
				ControllerServlet_V2.transTable2PL.put(dataElement, 0.0);
			}

			if (isExclusiveLockbyThisTransaction(dataElement) || isSharedLockbyThisTransaction(dataElement)) {
				returnString.add(0, ts + " --> " + dataElement + " = "
						+ ControllerServlet_V2.transTable2PL.get(dataElement).toString());
			} else {
				if (!ControllerServlet_V2.unlockStart.get(tranID)) {
					if (putSLock(dataElement)) {
						returnString.add(0, ts + " --> Shared lock for 'Read' --> " + dataElement + " = "
								+ ControllerServlet_V2.transTable2PL.get(dataElement).toString());
					}
				} else
					returnString.add(0, ts + "--> Illegal locking! No locks allowed after an unlock.");
			}

			if (ControllerServlet_V2.finalWrite2PL.containsKey(dataElement)) {
				ControllerServlet_V2.readFromRelation2PL.put(dataElement + "(" + Long.toString(tranID) + ")",
						ControllerServlet_V2.finalWrite2PL.get(dataElement));
			}

			break;
		case "expression":
			ArrayList<String> expressionSolution = transactionStmtTransformation.solveExpression(ts,
					ControllerServlet_V2.transTable2PL);
			if (expressionSolution.size() == 1) {
				returnString.add(0, ts + " --> " + expressionSolution.get(0));
			} else if (expressionSolution.size() > 1) {
				returnString.add(0, ts + " --> " + expressionSolution.get(0) + " = " + expressionSolution.get(1));
				returnString.add(1, expressionSolution.get(0));
				returnString.add(2, expressionSolution.get(1));
			}
			break;
		case "write":

			if (!ControllerServlet_V2.transTable2PL.containsKey(dataElement)) {
				ControllerServlet_V2.transTable2PL.put(dataElement, 0.0);
			}
			if (ControllerServlet_V2.expressionResultStorage2PL
					.containsKey(dataElement + "(" + Long.toString(tranID) + ")")) {

				returnString.add(0, write(ts, dataElement, ControllerServlet_V2.expressionResultStorage2PL
						.get(dataElement + "(" + Long.toString(tranID) + ")")));

				ControllerServlet_V2.expressionResultStorage2PL.remove(dataElement + "(" + Long.toString(tranID) + ")");

			} else {
				returnString.add(0, write(ts, dataElement, ControllerServlet_V2.transTable2PL.get(dataElement)));
			}

			ControllerServlet_V2.finalWrite2PL.put(dataElement, tranID);

			break;
		case "S_Lock":
			if (!ControllerServlet_V2.unlockStart.get(tranID)) {
				if (putSLock(dataElement)) {
					returnString.add(ts + " --> Shared lock aquired on '" + dataElement + "'");
				}
			} else
				returnString.add(0, ts + "--> Illegal locking! No locks allowed after an unlock.");
			break;
		case "X_Lock":
			if (!ControllerServlet_V2.unlockStart.get(tranID)) {
				if (putXLock(dataElement)) {
					returnString.add(ts + " --> Exclusive lock aquired on '" + dataElement + "'");
				}
			} else
				returnString.add(0, ts + "--> Illegal locking! No locks allowed after an unlock.");
			break;
		case "Unlock":
			if (ControllerServlet_V2.lockTable2PL.containsKey("S" + dataElement + Long.toString(tranID))
					|| ControllerServlet_V2.lockTable2PL.containsKey("X" + dataElement + Long.toString(tranID))) {
				ControllerServlet_V2.lockTable2PL.remove("S" + dataElement + Long.toString(tranID), tranID);
				ControllerServlet_V2.lockTable2PL.remove("X" + dataElement + Long.toString(tranID), tranID);
				ControllerServlet_V2.unlockStart.put(tranID, true);
				returnString.add(ts + " --> '" + dataElement + "' is unlocked!");
			} else {
				returnString.add(ts + " --> '" + dataElement + "' is not locked.");
			}
			break;
		case "abort":
			abort();
			returnString.add(0, ts + " --> <font color=\"red\"> <b>ABORTED transaction #" + Long.toString(tranID)
					+ " !</b></font> <br/>");
			returnString.add(1, "abort");
			break;
		}

		return returnString;

	}

	//////////////////////////////////// SUPPORTING PRIVATE
	//////////////////////////////////// METHODS///////////////////////////////////////////////////////

	/**
	 * Executes write(data-item) operation. Default value of data-item is 0.0, if
	 * not specified in previous operation with an expression like 'a = 12', where
	 * 'a' is the data-item.
	 * 
	 * @param ts
	 * @param dataElement
	 * @param value
	 * @return writes the value and returns success info.
	 * @throws InterruptedException - since it is programmed to wait for other
	 *                              threads to release the lock on the data-item, if
	 *                              any.
	 */
	private String write(String ts, String dataElement, double value) throws InterruptedException {

		String returnString = null;

		String oldValue = ""; // initialized to avoid null
		String newValue = Double.toString(value);

		// capture old value of data element from transTable
		if (ControllerServlet_V2.rollbackTable2PL.containsKey(dataElement + "(" + Long.toString(tranID) + ")")) {
			oldValue = ControllerServlet_V2.rollbackTable2PL.get(dataElement + "(" + Long.toString(tranID) + ")")
					.split("|")[0];
		} else {
			oldValue = ControllerServlet_V2.transTable2PL.get(dataElement).toString();
		}

		if (isExclusiveLockbyThisTransaction(dataElement)) {
			ControllerServlet_V2.transTable2PL.replace(dataElement, value);
			returnString = ts + " --> " + dataElement + " = "
					+ ControllerServlet_V2.transTable2PL.get(dataElement).toString();
		} else {
			if (!ControllerServlet_V2.unlockStart.get(tranID)) {
				if (putXLock(dataElement)) {
					ControllerServlet_V2.transTable2PL.replace(dataElement, value);
					returnString = ts + " --> Exclusive lock for 'Update' --> " + dataElement + " = "
							+ ControllerServlet_V2.transTable2PL.get(dataElement).toString();
				}
			} else
				returnString = ts + "--> Illegal locking! No locks allowed after an unlock.";
		}

		// add the before after state of the write operation, so the operations can be
		// undone when the transaction is aborted
		ControllerServlet_V2.rollbackTableTO.put(dataElement + "(" + Long.toString(tranID) + ")",
				oldValue + "|" + newValue);

		return returnString;

	}

	/**
	 * Puts shared lock (S-lock) for the data-item for current transaction ID if the
	 * following conditions are met: - no X-lock on data item by other
	 * transaction(s)
	 * 
	 * @param dataElement
	 * @return true - after acquiring the S-lock
	 * @throws InterruptedException - since it is programmed to wait for other
	 *                              threads to release the X-lock on the data-item,
	 *                              if any.
	 */
	private boolean putSLock(String dataElement) throws InterruptedException {
		synchronized (ControllerServlet_V2.lock) {
			while (isExclusiveLockbyOtherTransaction(dataElement)) {
				ControllerServlet_V2.lock.wait();
			}
			ControllerServlet_V2.lockTable2PL.put("S" + dataElement + Long.toString(tranID), tranID);
			return true;
		}
	}

	/**
	 * Puts exclusive lock (X-lock) for the data-item for current transaction ID if
	 * the following conditions are met: - no X-lock on data item by other
	 * transaction(s) - no S-lock on data-item by other transaction(s)
	 * 
	 * @param dataElement
	 * @return true - after acquiring the X-lock -> if there is S-lock for same
	 *         transaction, it is removed before acquiring X-lock
	 * @throws InterruptedException
	 */
	private boolean putXLock(String dataElement) throws InterruptedException {
		synchronized (ControllerServlet_V2.lock) {
			while (isSharedLockbyOtherTransaction(dataElement)) {
				ControllerServlet_V2.lock.wait();
			}
			while (isExclusiveLockbyOtherTransaction(dataElement)) {
				ControllerServlet_V2.lock.wait();
			}
			if (isSharedLockbyThisTransaction(dataElement)) {
				ControllerServlet_V2.lockTable2PL.remove("S" + dataElement + Long.toString(tranID), tranID);
			}
			ControllerServlet_V2.lockTable2PL.put("X" + dataElement + Long.toString(tranID), tranID);
			return true;
		}
	}

	/**
	 * Checks if S-locked by any other transaction(s)/thread(s).
	 * 
	 * @param dataElement
	 * @return true if yes, else false
	 */
	private boolean isSharedLockbyOtherTransaction(String dataElement) {

		boolean bool = false;
		for (long i : ControllerServlet_V2.tranIDList) {
			if (ControllerServlet_V2.lockTable2PL.containsKey("S" + dataElement + Long.toString(i))
					&& ControllerServlet_V2.lockTable2PL.get("S" + dataElement + Long.toString(i)) != tranID) {
				bool = true;
				break;
			} else {
				bool = false;
			}
		}
		return bool;

	}

	/**
	 * Checks if X-locked by any other transaction(s)/thread(s).
	 * 
	 * @param dataElement
	 * @return true if yes, else false
	 */
	private boolean isExclusiveLockbyOtherTransaction(String dataElement) {

		boolean bool = false;
		for (long i : ControllerServlet_V2.tranIDList) {
			if (ControllerServlet_V2.lockTable2PL.containsKey("X" + dataElement + Long.toString(i))
					&& ControllerServlet_V2.lockTable2PL.get("X" + dataElement + Long.toString(i)) != tranID) {
				bool = true;
				break;
			} else {
				bool = false;
			}
		}
		return bool;
	}

	/**
	 * Checks if X-locked by the current transaction/thread.
	 * 
	 * @param dataElement
	 * @return true if yes, else false
	 */
	private boolean isExclusiveLockbyThisTransaction(String dataElement) {

		if (ControllerServlet_V2.lockTable2PL.containsKey("X" + dataElement + Long.toString(tranID))) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Checks if S-locked by the current transaction/thread.
	 * 
	 * @param dataElement
	 * @return true if yes, else false
	 */
	private boolean isSharedLockbyThisTransaction(String dataElement) {

		if (ControllerServlet_V2.lockTable2PL.containsKey("S" + dataElement + Long.toString(tranID))) {
			return true;
		} else {
			return false;
		}
	}

	private void abort() {
		// ** undo 'write' modifications by current tranID since it is aborted
		Iterator<String> keySet = ControllerServlet_V2.rollbackTable2PL.keySet().iterator();

		while (keySet.hasNext()) {
			String key = keySet.next();
			if (key.contains("(" + tranID + ")")) {

				double oldVal = Double.parseDouble(ControllerServlet_V2.rollbackTable2PL.get(key).split("|")[0]);
				// double newVal =
				// Double.parseDouble(ControllerServlet_V2.rollbackTableTO.get(key).split("|")[1]);
				String dataElement = key.replace("(" + tranID + ")", "");
				ControllerServlet_V2.transTableTO.replace(dataElement, oldVal);
				// This statement ensures that if, another transaction wrote the data item, they
				// will not loose their modifications.
				// ControllerServlet_V2.transTableTO.replace(dataElement, newVal, oldVal);
			}
		}

		ControllerServlet_V2.abortedTransactions2PL.putIfAbsent(tranID, "aborted");
	}

	private void abortIfReadingFromAbortedTransaction() {
		if (ControllerServlet_V2.readOnlyTransaction2PL.containsKey(tranID)
				&& ControllerServlet_V2.readOnlyTransaction2PL.get(tranID) == false) {

			Iterator<String> readRelationInterator = ControllerServlet_V2.readFromRelation2PL.keySet().iterator();

			while (readRelationInterator.hasNext()) {
				String key = readRelationInterator.next();
				if (key.contains("(" + tranID + ")")) {

					long readingFromTranID = ControllerServlet_V2.readFromRelation2PL.get(key);

					if (ControllerServlet_V2.abortedTransactions2PL.containsKey(readingFromTranID)) {
						ControllerServlet_V2.abortedTransactions2PL.putIfAbsent(tranID,
								"aborted because it is reading from an aborted transaction #"
										+ Long.toString(readingFromTranID));
						abort();
					}
				}
				readRelationInterator.remove();
			}
		}
	}

}
