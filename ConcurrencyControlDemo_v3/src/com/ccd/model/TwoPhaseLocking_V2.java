package com.ccd.model;

import java.util.ArrayList;
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

		switch (operationType) {
		case "read":
			if (ControllerServlet_V2.transTable.containsKey(dataElement)) {

				if (isExclusiveLockbyThisTransaction(dataElement) || isSharedLockbyThisTransaction(dataElement)) {
					returnString.add(0, ts + " --> " + dataElement + " = "
							+ ControllerServlet_V2.transTable.get(dataElement).toString());
				} else {
					if (!ControllerServlet_V2.unlockStart.get(tranID)) {
						if (putSLock(dataElement)) {
							returnString.add(0, ts + " --> Shared lock for 'Read' --> " + dataElement + " = "
									+ ControllerServlet_V2.transTable.get(dataElement).toString());
						}
					} else
						returnString.add(0, ts + "--> Illegal locking! No locks allowed after an unlock.");
				}

			} else {
				if (!ControllerServlet_V2.unlockStart.get(tranID)) {
					ControllerServlet_V2.lockTable.put("S" + dataElement + Long.toString(tranID), tranID);
					ControllerServlet_V2.transTable.put(dataElement, 0.0);
					returnString.add(0, ts + " --> Share lock for 'Read' --> " + dataElement + " = "
							+ ControllerServlet_V2.transTable.get(dataElement).toString());
					/*
					 * // capture state of table for abort operation
					 * ControllerServlet_V2.rollbackTable.put(dataElement + "tranID", 0.0);
					 */
				} else
					returnString.add(0, ts + "--> Illegal locking! No locks allowed after an unlock.");
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
			/*
			 * // capture state of table before latest "write" --- for successful abort
			 * later // ControllerServlet_V2.rollbackTable.put(dataElement + "tranID",
			 * ControllerServlet_V2.transTable.get(dataElement));
			 */
			if (!ControllerServlet_V2.transTable.containsKey(dataElement)) {
				ControllerServlet_V2.transTable.put(dataElement, 0.0);
			}
			if (ControllerServlet_V2.tempTable.containsKey(dataElement + Long.toString(tranID))) {
				returnString.add(0, write(ts, dataElement,
						ControllerServlet_V2.tempTable.get(dataElement + Long.toString(tranID))));
				ControllerServlet_V2.tempTable.remove(dataElement + Long.toString(tranID));
			} else {
				returnString.add(0, write(ts, dataElement, ControllerServlet_V2.transTable.get(dataElement)));
			}
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
			synchronized (ControllerServlet_V2.lock) {
				ControllerServlet_V2.unlockStart.put(tranID, true);
				ControllerServlet_V2.lockTable.remove("S" + dataElement + Long.toString(tranID), tranID);
				ControllerServlet_V2.lockTable.remove("X" + dataElement + Long.toString(tranID), tranID);
				returnString.add(ts + " --> '" + dataElement + "' is unlocked");
				ControllerServlet_V2.lock.notifyAll();
			}
			break;
		/*
		 * case "Abort":
		 * 
		 * break;
		 */
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

		if (isExclusiveLockbyThisTransaction(dataElement)) {
			ControllerServlet_V2.transTable.replace(dataElement, value);
			returnString = ts + " --> " + dataElement + " = "
					+ ControllerServlet_V2.transTable.get(dataElement).toString();
		} else {
			if (!ControllerServlet_V2.unlockStart.get(tranID)) {
				if (putXLock(dataElement)) {
					ControllerServlet_V2.transTable.replace(dataElement, value);
					returnString = ts + " --> Exclusive lock for 'Update' --> " + dataElement + " = "
							+ ControllerServlet_V2.transTable.get(dataElement).toString();
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

}
