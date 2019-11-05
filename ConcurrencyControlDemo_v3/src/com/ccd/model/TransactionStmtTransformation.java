package com.ccd.model;

import java.util.ArrayList;
import java.util.HashMap;

public class TransactionStmtTransformation {
	// find the type of operation, i.e., whether it is read, write or is an
	// expression
	public String operationType(String ts) {

		if (ts.toLowerCase().contains("read(") || ts.toLowerCase().contains("r(")) {
			return "read";
		} else if (ts.toLowerCase().contains("write(") || ts.toLowerCase().contains("w(")) {
			return "write";
		} else {
			return "expression";
		}

	}

	// in operations like 'read(x)' and 'write(y)', find which dataItem is involved
	public String dataElement(String ts, String operationType) {
		String c = null;
		if (operationType == "read" || operationType == "write") {
			int index = ts.indexOf("(", 0);
			c = Character.toString(ts.charAt(index + 1));
		}
		return c;
	}

	/*
	 * public ArrayList<Character> dataItems(String ts) { ArrayList<Character>
	 * dataItems = new ArrayList<Character>();
	 * 
	 * for (int i = 0; i < ts.length(); i++) { switch (ts.charAt(i)) { case 'x': if
	 * (!dataItems.contains('x')) { dataItems.add('x'); } case 'y': if
	 * (!dataItems.contains('y')) { dataItems.add('y'); } case 'z': if
	 * (!dataItems.contains('z')) { dataItems.add('z'); } } } return dataItems; }
	 */

	/**
	 * To perform arithmetic operations for equations with more than 2 operands
	 * 
	 * @param ts         - Input string from the client browser through jsp page
	 * @param transTable - Table holding all the data items and their values that
	 *                   have been read with 'read()' operation; key = data item,
	 *                   value = value of data item.
	 * @return ArrayList<String> - holds the data item key in index = 0 and value in
	 *         index = 1. If any value is missing (null) in the expression, a
	 *         message is placed at index = 0.
	 */
	public ArrayList<String> solveExpression(String ts, HashMap<String, Double> transTable) {
		String ts1 = null;

		// will store data item and its value, e.g., x and value of x
		ArrayList<String> resultArray = new ArrayList<String>();
		double result = 0;
		ArrayList<Double> equation = new ArrayList<Double>();
		ArrayList<String> operator = new ArrayList<String>();

		// Remove spaces
		ts = ts.replace(" ", "");

		// extract operands x, y and numbers
		String[] operands = ts.split("\\p{Punct}");

		// Remove any number of digits with just one alphabet 'q', i.e., 100 with 'q' or
		// 1 with 'q'
		// this will help in extracting arithmetic operators with split function without
		// any unwanted empty indices
		ts1 = ts.replaceAll("[0-9]+", "q");

		// extract assignment and arithmetic operators
		String[] operators = ts1.split("[qxyz]");

		resultArray.add(0, operands[0]);
		// store values for variables, in the incoming equation "ts", in integer form
		// to perform calculations later
		for (int i = 1; i < operands.length; i++) {
			if (transTable.containsKey(operands[i].trim())) {
				equation.add(transTable.get(operands[i].trim()));
			} else if (operands[i].trim().matches("\\d+")) {
				equation.add(Double.parseDouble(operands[i].trim()));
			} else {
				resultArray.set(0, "Value for the data item " + operands[i].trim() + "  is missing!");
				return resultArray;
			}
		}

		// store operators +,-,*,/ from the incoming equation "ts" into ArrayList
		// 'operator'
		if (operands.length == 2) {
			resultArray.add(1, equation.get(0).toString());
			return resultArray;
		} else if (operands.length > 2) {
			for (int op = 2; op < operators.length; op++) {
				operator.add(0, operators[op].trim());
			}
		}

		// performing arithmetic operation using BODMAS in next few 'for loops'

		// division
		for (int k = 0; k < operator.size(); k++) {
			if (operator.get(k).contains("/")) {
				result = equation.get(k) / equation.get(k + 1);
				equation.set(k, result);
				equation.set(k + 1, result);
			}
		}

		// multiplication
		for (int k = 0; k < operator.size(); k++) {
			if (operator.get(k).contains("*")) {
				result = equation.get(k) * equation.get(k + 1);
				equation.set(k, result);
				equation.set(k + 1, result);
			}
		}

		// addition
		for (int k = 0; k < operator.size(); k++) {
			if (operator.get(k).contains("+")) {
				result = equation.get(k) + equation.get(k + 1);
				equation.set(k, result);
				equation.set(k + 1, result);
			}
		}

		// subtraction
		for (int k = 0; k < operator.size(); k++) {
			if (operator.get(k).contains("-")) {
				result = equation.get(k) - equation.get(k + 1);
			}
		}

		resultArray.add(1, Double.toString(result));

		return resultArray;

	}
	
}
