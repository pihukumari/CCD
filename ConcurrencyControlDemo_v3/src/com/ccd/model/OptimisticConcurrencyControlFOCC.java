package com.ccd.model;

import java.util.ArrayList;
import java.util.Iterator;

import com.ccd.controler.ControllerServlet_V2;

public class OptimisticConcurrencyControlFOCC {

	long tranID;

	public OptimisticConcurrencyControlFOCC(long tranID) {
		super();
		this.tranID = tranID;
	}

	public ArrayList<String> transactionResult(String ts) throws InterruptedException {

		ArrayList<String> returnString = new ArrayList<String>();

		TransactionStmtTransformation transactionStmtTransformation = new TransactionStmtTransformation();
		String operationType = transactionStmtTransformation.operationType(ts);
		String dataElement = transactionStmtTransformation.dataElement(ts, operationType);

		// find if any other transaction is in val-write phase
		Iterator<Long> tranIterator = ControllerServlet_V2.transactionPhaseFOCC.keySet().iterator();
		synchronized (ControllerServlet_V2.lock) {

			while (tranIterator.hasNext()) {
				Long tranIDinValWrite = tranIterator.next();

				while (ControllerServlet_V2.transactionPhaseFOCC.get(tranIDinValWrite) == "val-write phase"
						&& tranIDinValWrite != tranID) {
					// wait if other transaction is in val-write phase
					ControllerServlet_V2.lock.wait();
				}
			}

		}

		// else enter read phase for current transaction and execute operations stmnt
		ControllerServlet_V2.transactionPhaseFOCC.putIfAbsent(tranID, "read phase");

		switch (operationType) {
		case "read":

			// update read set of current transaction
			ControllerServlet_V2.readSetFOCC.put(dataElement + "(" + Long.toString(tranID) + ")", tranID);

			// read the data
			if (ControllerServlet_V2.transTableFOCC.containsKey(dataElement)) {
				returnString.add(0, ts + " --> " + dataElement + " = "
						+ ControllerServlet_V2.transTableFOCC.get(dataElement).toString());

			} else {
				ControllerServlet_V2.transTableFOCC.put(dataElement, 0.0);
				returnString.add(0, ts + " --> " + dataElement + " = "
						+ ControllerServlet_V2.transTableFOCC.get(dataElement).toString());
			}
			break;
		case "expression":
			ArrayList<String> expressionSolution = transactionStmtTransformation.solveExpression(ts,
					ControllerServlet_V2.transTableFOCC);
			if (expressionSolution.size() == 1) {
				returnString.add(0, ts + " --> " + expressionSolution.get(0));
			} else if (expressionSolution.size() > 1) {
				returnString.add(0, ts + " --> " + expressionSolution.get(0) + " = " + expressionSolution.get(1));
				returnString.add(1, expressionSolution.get(0));
				returnString.add(2, expressionSolution.get(1));
			}
			break;
		case "write":

			// Add the data item to private workspace table if it does not have the data
			// item for
			// write operation
			if (!ControllerServlet_V2.privateWorkspaceFOCC
					.containsKey(dataElement + "(" + Long.toString(tranID) + ")")) {
				ControllerServlet_V2.privateWorkspaceFOCC.put(dataElement + "(" + Long.toString(tranID) + ")", 0.0);
			}

			if (ControllerServlet_V2.expressionResultStorageFOCC.containsKey(dataElement + "(" + Long.toString(tranID) + ")")) {

				returnString.add(0, write(ts, dataElement,
						ControllerServlet_V2.expressionResultStorageFOCC.get(dataElement + "(" + Long.toString(tranID) + ")")));
				ControllerServlet_V2.expressionResultStorageFOCC.remove(dataElement + "(" + Long.toString(tranID) + ")");
			} else {
				returnString.add(0, write(ts, dataElement, ControllerServlet_V2.privateWorkspaceFOCC
						.get(dataElement + "(" + Long.toString(tranID) + ")")));
			}

			// update write set for current transaction
			ControllerServlet_V2.writeSetFOCC.put(dataElement + "(" + Long.toString(tranID) + ")", tranID);

			break;
		case "abort":
			abort(tranID, "self aborted");
			returnString.add(0, "<br/> ABORTED Transaction# " + Long.toString(tranID));
			returnString.add(1, "aborted");
			break;
		case "abort conflicting transaction":
			abort(ControllerServlet_V2.conflictingTranID, "aborted by transaction#" + Long.toString(tranID));
			returnString.add(0,
					"<br/> ABORTED Conflicting Transaction# " + Long.toString(ControllerServlet_V2.conflictingTranID)
							+ " ! <br/> Please try to commit this transaction again!");
			break;
		}

		return returnString;
	}

	/**
	 * Writes the modifications to the private workspace of the current transaction.
	 * 
	 * @param ts
	 * @param dataElement
	 * @param value
	 * @return
	 */
	private String write(String ts, String dataElement, double value) {

		String returnString = null;

		ControllerServlet_V2.privateWorkspaceFOCC.replace(dataElement + "(" + Long.toString(tranID) + ")", value);
		returnString = ts + " --> " + dataElement + " = " + ControllerServlet_V2.privateWorkspaceFOCC
				.get(dataElement + "(" + Long.toString(tranID) + ")").toString();
		return returnString;

	}

	/**
	 * Validates a transaction.
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public String validateTransaction() throws InterruptedException {

		boolean isNotReadOnlyTransaction = false;
		if (ControllerServlet_V2.writeSetFOCC.containsValue(tranID)) {
			isNotReadOnlyTransaction = true;
		}

		if (isNotReadOnlyTransaction) {
			// find if any other transaction is in val-write phase
			Iterator<Long> tranIterator = ControllerServlet_V2.transactionPhaseFOCC.keySet().iterator();
			synchronized (ControllerServlet_V2.lock) {

				while (tranIterator.hasNext()) {
					Long tranIDinValWrite = tranIterator.next();

					if (ControllerServlet_V2.transactionPhaseFOCC.get(tranIDinValWrite) == "val-write phase"
							&& tranIDinValWrite != tranID) {
						// wait if other transaction is in val-write phase
						ControllerServlet_V2.lock.wait();
					} else {
						// else enter val-write phase for current transaction
						ControllerServlet_V2.transactionPhaseFOCC.put(tranID, "val-write phase");
					}
				}

			}

			if (ControllerServlet_V2.transactionPhaseFOCC.get(tranID) == "val-write phase") {
				// iterate to find the previously validated transactions
				// Iterator<Long> tranIterator =
				// ControllerServlet_V2.transactionPhaseFOCC.keySet().iterator();
				Iterator<Long> tranIDIterator = ControllerServlet_V2.transactionPhaseFOCC.keySet().iterator();
				while (tranIDIterator.hasNext()) {
					Long tranIDinReadPhase = tranIDIterator.next();

					// if transaction(s) in read-phase exist
					if (ControllerServlet_V2.transactionPhaseFOCC.get(tranIDinReadPhase) == "read phase") {

						// iterate through the read set of other active transaction(s)
						Iterator<String> readSet = ControllerServlet_V2.readSetFOCC.keySet().iterator();
						while (readSet.hasNext()) {
							String rsKey = readSet.next();

							// extract data item involved
							String dataitem = rsKey.replace("(" + Long.toString(tranIDinReadPhase) + ")", "");

							// check of intersection of read set and write set is empty
							if (rsKey.contains("(" + Long.toString(tranIDinReadPhase) + ")")) { // read-set of other
																								// active
																								// transactions (in read
																								// their read-phase)
								if (ControllerServlet_V2.writeSetFOCC
										.containsKey(dataitem + "(" + Long.toString(tranID) + ")")) { // conflicting
																										// write-set
																										// from
																										// validating
																										// transaction
									ControllerServlet_V2.conflictingTranID = tranIDinReadPhase;
									return Long.toString(tranIDinReadPhase);
								}
							}
						}
					}
				}
				// if the validation is successful, then it has finish the write and update the
				// phase table
				if (ControllerServlet_V2.transactionPhaseFOCC.get(tranID) == "val-write phase") {
					ControllerServlet_V2.transactionPhaseFOCC.replace(tranID, "validated");
					writeToDB(); // write the modification of write operation
				}
			}
		} else {
			ControllerServlet_V2.transactionPhaseFOCC.replace(tranID, "committed");
		}
		return null;
	}

	/**
	 * Writes the modifications by a transaction (once if it is successfully
	 * validated) from private workspace to the database, making the values
	 * permanent and accessible for other transactions
	 */
	private void writeToDB() {

		Iterator<String> privateWorkspaceIterator = ControllerServlet_V2.privateWorkspaceFOCC.keySet().iterator();

		while (privateWorkspaceIterator.hasNext()) {
			String key = privateWorkspaceIterator.next();
			if (key.contains("(" + Long.toString(tranID) + ")")) {
				String dataitem = key.replace("(" + Long.toString(tranID) + ")", "");

				ControllerServlet_V2.transTableFOCC.put(dataitem, ControllerServlet_V2.privateWorkspaceFOCC.get(key));

				// clear private workspace by removing used data
				privateWorkspaceIterator.remove();
				//ControllerServlet_V2.privateWorkspaceFOCC.remove(key);
				//privateWorkspaceIterator = ControllerServlet_V2.privateWorkspaceFOCC.keySet().iterator();
			}
		}

	}

	/**
	 * Aborts the specified transaction, i.e., clears the private workspace,
	 * read-set and write-set of the specified transaction. Also sets the phase of
	 * the transaction to specified "phaseState".
	 * 
	 * @param tranID
	 * @param phaseState - String parameter 
	 */
	private void abort(Long tranID, String phaseState) {

		// aborted according to // FOCC validation rule // clear the workspace, i.e,
			// discard the modifications
			Iterator<String> privateWorkspaceIterator = ControllerServlet_V2.privateWorkspaceFOCC.keySet().iterator();

			while (privateWorkspaceIterator.hasNext()) {
				String key = privateWorkspaceIterator.next();
				if (key.contains("(" + Long.toString(tranID) + ")")) {
					privateWorkspaceIterator.remove();
					//ControllerServlet_V2.privateWorkspaceFOCC.remove(key);
					//privateWorkspaceIterator = ControllerServlet_V2.privateWorkspaceFOCC.keySet().iterator();
				}
			}

			// set the current transaction as aborted in the phase table
			ControllerServlet_V2.transactionPhaseFOCC.replace(tranID, phaseState);

			// remove read set for this aborted transaction
			Iterator<String> readSet = ControllerServlet_V2.readSetFOCC.keySet().iterator();
			while (readSet.hasNext()) {
				String rsKey = readSet.next();
				if (rsKey.contains("(" + Long.toString(tranID) + ")")) {
					readSet.remove();
					//ControllerServlet_V2.readSetFOCC.remove(rsKey);
					//readSet = ControllerServlet_V2.readSetFOCC.keySet().iterator();
				}
			}
			// remove write set for this aborted transaction
			Iterator<String> writeSet = ControllerServlet_V2.writeSetFOCC.keySet().iterator();
			while (writeSet.hasNext()) {
				String wsKey = writeSet.next();
				if (wsKey.contains("(" + Long.toString(tranID) + ")")) {
					writeSet.remove();
					//ControllerServlet_V2.writeSetFOCC.remove(wsKey);
					//readSet = ControllerServlet_V2.writeSetFOCC.keySet().iterator();
				}
			}
	}

}
