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

		// check if current transaction has dirty reads. If yes, abort it
		if (operationType != "abort") {
			abortIfReadingFromAbortedTransaction();
		}
		// if it is aborted in the above statement, send output message to servlet. Else
		// continue normal execution
		if (ControllerServlet_V2.abortedTransactionsList2PL.containsKey(tranID)) {
			returnString.add(0, "<font color=\"red\">This transaction #" + Long.toString(tranID) + " has been "
					+ ControllerServlet_V2.abortedTransactionsList2PL.get(tranID) + "!</font>");
			returnString.add(1, "abort");
			return returnString;
		}

		// add data element to the transTable if it doesn't already exists
		ControllerServlet_V2.transTable2PL.putIfAbsent(dataElement, 0.0);

		switch (operationType) {

		// handle read operations
		case "read":

			if (isExclusiveLockbyThisTransaction(dataElement) || isSharedLockbyThisTransaction(dataElement)) {
				// if transaction already has a S or X lock
				returnString.add(0, ts + " --> " + dataElement + " = "
						+ ControllerServlet_V2.transTable2PL.get(dataElement).toString());
			} else {
				// else acquire the S lock, but first check if the transaction is still in
				// growing phase
				if (!ControllerServlet_V2.unlockStart.get(tranID)) {
					// if in growing phase, get the lock and read data
					if (putSLock(dataElement)) {
						returnString.add(0, ts + " --> Shared lock for 'Read' --> " + dataElement + " = "
								+ ControllerServlet_V2.transTable2PL.get(dataElement).toString());
					}
				} else
					// else reject the operation with a message
					returnString.add(0, ts + "--> Illegal locking! No locks allowed after an unlock.");
			}

			// update readfromRelation table with information about from where this
			// transaction is reading the data -> from its own write or from write of
			// another transaction
			if (ControllerServlet_V2.finalWrite2PL.containsKey(dataElement)) {
				ControllerServlet_V2.readFromRelation2PL.put(dataElement + "(" + Long.toString(tranID) + ")",
						ControllerServlet_V2.finalWrite2PL.get(dataElement));
			}

			break;

		// handle expressions/equations
		case "expression":
			ArrayList<String> expressionSolution = transactionStmtTransformation.solveExpression(ts,
					ControllerServlet_V2.transTable2PL);
			
			if (expressionSolution.size() == 1) {
				// to handle the message about missing data item
				returnString.add(0, ts + " --> " + expressionSolution.get(0));
				
			} else if (expressionSolution.size() > 1) {
				// prepare the output message to servlet
				returnString.add(0, ts + " --> " + expressionSolution.get(0) + " = " + expressionSolution.get(1));
				
				// store values (calculated from expressions) to be used in write operations
				// later
				ControllerServlet_V2.expressionResultStorage2PL.put(
						expressionSolution.get(0) + "(" + Long.toString(tranID) + ")",
						Double.parseDouble(expressionSolution.get(1)));
			}
			break;

		// handle write operations
		case "write":

			// check if user has assigned a value to data item using an expression
			if (ControllerServlet_V2.expressionResultStorage2PL
					.containsKey(dataElement + "(" + Long.toString(tranID) + ")")) {
				// if yes, write that value
				returnString.add(0, write(ts, dataElement, ControllerServlet_V2.expressionResultStorage2PL
						.get(dataElement + "(" + Long.toString(tranID) + ")")));

				ControllerServlet_V2.expressionResultStorage2PL.remove(dataElement + "(" + Long.toString(tranID) + ")");

			} else {
				// if not, simply rewrite the value from transTable
				returnString.add(0, write(ts, dataElement, ControllerServlet_V2.transTable2PL.get(dataElement)));
			}

			// update finalWrite table with info that which transaction has finally written
			// the data item
			ControllerServlet_V2.finalWrite2PL.put(dataElement, tranID);

			break;

		// handle explicit request for shared lock
		case "S_Lock":
			if (!ControllerServlet_V2.unlockStart.get(tranID)) {
				// if in growing phase, grant the lock
				if (putSLock(dataElement)) {
					returnString.add(ts + " --> Shared lock aquired on '" + dataElement + "'");
				}
			} else
				// else reject the request
				returnString.add(0, ts + "--> Illegal locking! No locks allowed after an unlock.");
			break;

		// handle explicit request for exclusive lock
		case "X_Lock":
			if (!ControllerServlet_V2.unlockStart.get(tranID)) {
				// if in growing phase, grant the lock
				if (putXLock(dataElement)) {
					returnString.add(ts + " --> Exclusive lock aquired on '" + dataElement + "'");
				}
			} else
				// else reject the request
				returnString.add(0, ts + "--> Illegal locking! No locks allowed after an unlock.");
			break;

		// handle explicit request for unlocking a data item
		case "Unlock":
			if (ControllerServlet_V2.lockTable.containsKey("S" + dataElement + Long.toString(tranID))
					|| ControllerServlet_V2.lockTable.containsKey("X" + dataElement + Long.toString(tranID))) {
				ControllerServlet_V2.lockTable.remove("S" + dataElement + Long.toString(tranID), tranID);
				ControllerServlet_V2.lockTable.remove("X" + dataElement + Long.toString(tranID), tranID);
				ControllerServlet_V2.unlockStart.put(tranID, true);
				returnString.add(ts + " --> '" + dataElement + "' is unlocked!");
			} else {
				returnString.add(ts + " --> '" + dataElement + "' is not locked.");
			}
			break;

		// handle the request to abort the transaction
		case "abort":

			abort(); // reverts the effects of writes by current transaction
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

		// add the before state of the write operation, so the operations can be
		// undone when the transaction is aborted
		ControllerServlet_V2.rollbackTable2PL.putIfAbsent(dataElement + "(" + Long.toString(tranID) + ")",
				ControllerServlet_V2.transTable2PL.get(dataElement));

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
			ControllerServlet_V2.lockTable.put("S" + dataElement + Long.toString(tranID), tranID);
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
				ControllerServlet_V2.lockTable.remove("S" + dataElement + Long.toString(tranID), tranID);
			}
			ControllerServlet_V2.lockTable.put("X" + dataElement + Long.toString(tranID), tranID);
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
			if (ControllerServlet_V2.lockTable.containsKey("S" + dataElement + Long.toString(i))
					&& ControllerServlet_V2.lockTable.get("S" + dataElement + Long.toString(i)) != tranID) {
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
			if (ControllerServlet_V2.lockTable.containsKey("X" + dataElement + Long.toString(i))
					&& ControllerServlet_V2.lockTable.get("X" + dataElement + Long.toString(i)) != tranID) {
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

		if (ControllerServlet_V2.lockTable.containsKey("X" + dataElement + Long.toString(tranID))) {
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

		if (ControllerServlet_V2.lockTable.containsKey("S" + dataElement + Long.toString(tranID))) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * It is called when abort request is executed. It reverses the effects of
	 * writes and removes final write set of the aborting transaction. And adds it
	 * to the list of aborted transactions, so that application can detect dirty
	 * reads for other transactions and hence can abort them too.
	 * 
	 * @param tranID
	 */
	private void abort() {

		// ** undo 'write' modifications by current tranID since it is aborted
		Iterator<String> keySet = ControllerServlet_V2.rollbackTable2PL.keySet().iterator();

		while (keySet.hasNext()) {
			String key = keySet.next();
			String dataElement = null;
			if (key.contains("(" + tranID + ")")) {

				double oldVal = ControllerServlet_V2.rollbackTable2PL.get(key);
				dataElement = key.replace("(" + tranID + ")", "");
				ControllerServlet_V2.transTable2PL.replace(dataElement, oldVal);
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
		Iterator<String> finalWritekeySet = ControllerServlet_V2.finalWrite2PL.keySet().iterator();

		while (keySet.hasNext()) {
			String key = keySet.next();
			if (ControllerServlet_V2.finalWrite2PL.get(key) == tranID) {
				finalWritekeySet.remove();
			}
		}

		// add the current transaction to aborted list.
		ControllerServlet_V2.abortedTransactionsList2PL.putIfAbsent(tranID, "aborted");
	}

	/**
	 * This method is called to cause cascade abort in case of a dirty read.
	 */
	private void abortIfReadingFromAbortedTransaction() {

		Iterator<String> readRelationInterator = ControllerServlet_V2.readFromRelation2PL.keySet().iterator();

		while (readRelationInterator.hasNext()) {
			String key = readRelationInterator.next();
			if (key.contains("(" + tranID + ")")) {

				long readingFromTranID = ControllerServlet_V2.readFromRelation2PL.get(key);

				if (ControllerServlet_V2.abortedTransactionsList2PL.containsKey(readingFromTranID)) {
					ControllerServlet_V2.abortedTransactionsList2PL.putIfAbsent(tranID,
							"aborted because it is reading from an aborted transaction #"
									+ Long.toString(readingFromTranID));

					abort();
				}
			}
		}
	}

}
