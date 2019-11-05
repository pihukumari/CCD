package com.ccd.model;

import java.util.ArrayList;
import java.util.HashMap;

import com.ccd.model.TransactionStmtTransformation;
import org.apache.poi.xssf.usermodel.XSSFSheet;

public class TwoPhaseLocking {

	// set if class attributes to retain the values read from database
	/*
	 * double x_fromDB = 0; double y_fromDB = 0; double z_fromDB = 0;
	 */
	// hash map to store modified values after solving expressions entered by the
	// user
	// HashMap<Character, Double> modifiedValue = new HashMap<Character, Double>();

	XSSFSheet transTable;
	XSSFSheet lockTable;
	HashMap<String, Double> transHashMap = new HashMap<String, Double>();

	public TwoPhaseLocking(XSSFSheet tranTable, XSSFSheet locksTable, HashMap<String, Double> transHM) {
		super();

		// Get desired sheets from the workbook
		transTable = tranTable;
		lockTable = locksTable;
		transHashMap = transHM;

	}

	public ArrayList<String> transactionResult(String ts) {

		ArrayList<String> returnString = new ArrayList<String>();

		TransactionStmtTransformation stmtTransformation = new TransactionStmtTransformation();
		String operationType = stmtTransformation.operationType(ts);
		String dataElement = stmtTransformation.dataElement(ts, operationType);

		switch (operationType) {
		case "read":
			switch (dataElement) {
			case "x":
				transHashMap.put("x", transTable.getRow(1).getCell(0).getNumericCellValue());
				/*
				 * double x = transHashMap.get("x"); double xx =
				 * transTable.getRow(1).getCell(0).getNumericCellValue(); System.out.println(x);
				 * System.out.println(xx);
				 */
				returnString.add(0, ts + " --> x = " + transHashMap.get("x").toString());
				returnString.add(1, "x");
				returnString.add(2, transHashMap.get("x").toString());
				break;
			case "y":
				transHashMap.put("y", transTable.getRow(1).getCell(1).getNumericCellValue());
				returnString.add(0, ts + " --> y = " + transHashMap.get("y").toString());
				returnString.add(1, "y");
				returnString.add(2, transHashMap.get("y").toString());
				break;
			case "z":
				transHashMap.put("z", transTable.getRow(1).getCell(2).getNumericCellValue());
				returnString.add(0, ts + " --> z = " + transHashMap.get("z").toString());
				returnString.add(1, "z");
				returnString.add(2, transHashMap.get("z").toString());
				break;
			}

			break;
		case "expression":

			ArrayList<String> expression = stmtTransformation.solveExpression(ts, transHashMap);
			if (expression.size() == 1) {
				returnString.add(0, ts + " --> " + expression.get(0));
			} else if (expression.size() > 1) {
				returnString.add(0, ts + " --> " + expression.get(0).replace("m", "") + " = " + expression.get(1));
				returnString.add(1, expression.get(0));
				returnString.add(2, expression.get(1));
			}
			break;

		case "write":
			switch (dataElement) {
			case "x":
				transTable.getRow(1).getCell(0).setCellValue(transHashMap.get("xm"));
				returnString.add(0, ts + " --> x = " + transHashMap.get("xm").toString());
				break;
			case "y":
				transTable.getRow(1).getCell(1).setCellValue(transHashMap.get("ym"));
				returnString.add(0, ts + " --> y = " + transHashMap.get("ym").toString());
				break;
			case "z":
				transTable.getRow(1).getCell(2).setCellValue(transHashMap.get("zm"));
				returnString.add(0, ts + " --> z = " + transHashMap.get("zm").toString());
				break;
			}
			break;
		}

		return returnString;

	}

}
