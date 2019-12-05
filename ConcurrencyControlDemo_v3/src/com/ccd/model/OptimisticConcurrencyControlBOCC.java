package com.ccd.model;

import java.util.ArrayList;
import java.util.Iterator;

import com.ccd.controler.ControllerServlet_V2;

public class OptimisticConcurrencyControlBOCC {

	long tranID;

	public OptimisticConcurrencyControlBOCC(long tranID) {
		super();
		this.tranID = tranID;
	}

	public ArrayList<String> transactionResult(String ts) {

		ArrayList<String> returnString = new ArrayList<String>();

		TransactionStmtTransformation transactionStmtTransformation = new TransactionStmtTransformation();
		String operationType = transactionStmtTransformation.operationType(ts);
		String dataElement = transactionStmtTransformation.dataElement(ts, operationType);

		switch (operationType) {
		case "read":

			// update Phase table
			ControllerServlet_V2.transactionPhaseBOCC.putIfAbsent(tranID, "read phase");
			// record begin of each transaction (BOT)
			ControllerServlet_V2.transLifeTime.putIfAbsent("BOT" + "(" + Long.toString(tranID) + ")",
					System.currentTimeMillis());

			// update read set of current transaction
			ControllerServlet_V2.readSetBOCC.put(dataElement + "(" + Long.toString(tranID) + ")", tranID);

			// read the data
			if (ControllerServlet_V2.transTableBOCC.containsKey(dataElement)) {
				returnString.add(0, ts + " --> " + dataElement + " = "
						+ ControllerServlet_V2.transTableBOCC.get(dataElement).toString());

			} else {
				ControllerServlet_V2.transTableBOCC.put(dataElement, 0.0);
				returnString.add(0, ts + " --> " + dataElement + " = "
						+ ControllerServlet_V2.transTableBOCC.get(dataElement).toString());
			}

			break;
		case "expression":
			ArrayList<String> expressionSolution = transactionStmtTransformation.solveExpression(ts,
					ControllerServlet_V2.transTableBOCC);
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
			if (!ControllerServlet_V2.privateWorkspaceBOCC
					.containsKey(dataElement + "(" + Long.toString(tranID) + ")")) {
				ControllerServlet_V2.privateWorkspaceBOCC.put(dataElement + "(" + Long.toString(tranID) + ")", 0.0);
			}

			if (ControllerServlet_V2.expressionResultStorageBOCC
					.containsKey(dataElement + "(" + Long.toString(tranID) + ")")) {

				returnString.add(0, write(ts, dataElement, ControllerServlet_V2.expressionResultStorageBOCC
						.get(dataElement + "(" + Long.toString(tranID) + ")")));
				ControllerServlet_V2.expressionResultStorageBOCC
						.remove(dataElement + "(" + Long.toString(tranID) + ")");
			} else {
				returnString.add(0, write(ts, dataElement, ControllerServlet_V2.privateWorkspaceBOCC
						.get(dataElement + "(" + Long.toString(tranID) + ")")));
			}

			break;
		case "abort":
			abort();
			returnString.add(0, "ABORTED Transaction# " + Long.toString(tranID));
			returnString.add(1, "aborted");
		}

		return returnString;
	}

	private String write(String ts, String dataElement, double value) {

		String returnString = null;

		ControllerServlet_V2.privateWorkspaceBOCC.replace(dataElement + "(" + Long.toString(tranID) + ")", value);
		returnString = ts + " --> " + dataElement + " = " + ControllerServlet_V2.privateWorkspaceBOCC
				.get(dataElement + "(" + Long.toString(tranID) + ")").toString();
		return returnString;

	}

	public void validateTransaction() throws InterruptedException {

		// find if any other transaction is in val-write phase
		Iterator<Long> tranIterator = ControllerServlet_V2.transactionPhaseBOCC.keySet().iterator();
		synchronized (ControllerServlet_V2.lock) {

			while (tranIterator.hasNext()) {
				Long tranIDinValWrite = tranIterator.next();

				if (ControllerServlet_V2.transactionPhaseBOCC.get(tranIDinValWrite) == "val-write phase"
						&& tranIDinValWrite != tranID) {
					// wait if other transaction is in val-write phase
					ControllerServlet_V2.lock.wait();
				} else {
					// else enter val-write phase for current transaction
					ControllerServlet_V2.transactionPhaseBOCC.put(tranID, "val-write phase");
				}
			}

		}

		if (ControllerServlet_V2.transactionPhaseBOCC.get(tranID) == "val-write phase") {
			// iterate to find the previously validated transactions
			Iterator<Long> tranIDIterator = ControllerServlet_V2.transactionPhaseBOCC.keySet().iterator();
			while (tranIDIterator.hasNext()) {
				Long validatedTranID = tranIDIterator.next();

				// if previously validated transaction exists
				if (ControllerServlet_V2.transactionPhaseBOCC.get(validatedTranID) == "validated") {

					// check for overlap
					boolean isOverlapping = ControllerServlet_V2.transLifeTime
							.get("BOT" + "(" + Long.toString(tranID) + ")") < ControllerServlet_V2.transLifeTime
									.get("EOT" + "(" + Long.toString(validatedTranID) + ")");

					if (isOverlapping) {
						// iterate through the read set of current transaction
						Iterator<String> readSet = ControllerServlet_V2.readSetBOCC.keySet().iterator();
						while (readSet.hasNext()) {
							String rsKey = readSet.next();

							// extract data item involved
							String dataitem = rsKey.replace("(" + Long.toString(tranID) + ")", "");

							// check of intersection of read set and write set is empty
							if (rsKey.contains("(" + Long.toString(tranID) + ")")) {
								if (ControllerServlet_V2.writeSetBOCC
										.containsKey(dataitem + "(" + Long.toString(validatedTranID) + ")")) { // conflicting
																												// write-set
																												// from
																												// validated
																												// transaction

									// since intersection is not empty, the current transaction is aborted according
									// to BOCC validation rule
									// clear the workspace, i.e, discard the modifications
									synchronized (tranIDIterator) {
										tranIDIterator.wait(5000);
									}
									abort(validatedTranID);
									break;

								}
							}
						}
					}
				}
			}

			// if the transaction is not aborted, then it has finished validation
			// successfully and the write can be made permanent
			if (ControllerServlet_V2.transactionPhaseBOCC.get(tranID) == "val-write phase") {
				synchronized (tranIDIterator) {
					tranIDIterator.wait(5000);
				}
				// write the modification of write operation
				writeToDB();
				ControllerServlet_V2.transactionPhaseBOCC.replace(tranID, "validated");
				// record the end of transaction (EOT)
				ControllerServlet_V2.transLifeTime.putIfAbsent("EOT" + "(" + Long.toString(tranID) + ")",
						System.currentTimeMillis());
			}
		}

	}

	/**
	 * Writes the modifications by a transaction (once it is successfully validated)
	 * from private workspace to the database, making the values permanent and
	 * accessible for other transactions
	 */
	private void writeToDB() {

		Iterator<String> privateWorkspaceIterator = ControllerServlet_V2.privateWorkspaceBOCC.keySet().iterator();

		while (privateWorkspaceIterator.hasNext()) {
			String key = privateWorkspaceIterator.next();
			if (key.contains("(" + Long.toString(tranID) + ")")) {
				String dataitem = key.replace("(" + Long.toString(tranID) + ")", "");

				// update write set for current transaction
				ControllerServlet_V2.writeSetBOCC.put(dataitem + "(" + Long.toString(tranID) + ")", tranID);

				ControllerServlet_V2.transTableBOCC.put(dataitem, ControllerServlet_V2.privateWorkspaceBOCC.get(key));

				// clear private workspace by removing used data
				privateWorkspaceIterator.remove();
			}
		}

	}

	private void abort() {

		// aborted according to // BOCC validation rule // clear the workspace, i.e,
		// discard the modifications
		Iterator<String> privateWorkspaceIterator = ControllerServlet_V2.privateWorkspaceBOCC.keySet().iterator();

		while (privateWorkspaceIterator.hasNext()) {
			String key = privateWorkspaceIterator.next();
			if (key.contains("(" + Long.toString(tranID) + ")")) {
				privateWorkspaceIterator.remove();
			}
		}

		// set the current transaction as aborted in the phase table
		ControllerServlet_V2.transactionPhaseBOCC.replace(tranID, "aborted");

		// remove read set for this aborted transaction
		Iterator<String> readSet = ControllerServlet_V2.readSetBOCC.keySet().iterator();
		while (readSet.hasNext()) {
			String rsKey = readSet.next();
			if (rsKey.contains("(" + Long.toString(tranID) + ")")) {
				readSet.remove();
			}
		}
		// remove write set for this aborted transaction
		Iterator<String> writeSet = ControllerServlet_V2.writeSetBOCC.keySet().iterator();
		while (writeSet.hasNext()) {
			String wsKey = writeSet.next();
			if (wsKey.contains("(" + Long.toString(tranID) + ")")) {
				writeSet.remove();
			}
		}
	}

	private void abort(long validatedTranID) {

		abort();
		// set the reason for abort in the phase table - so it can be used in
		// ControllerServlet to be sent as a detailed response to the client
		ControllerServlet_V2.transactionPhaseBOCC.replace(tranID,
				" because it's Read-Set intersects with Write-Set of a validated transaction #" + validatedTranID);
	}

}
