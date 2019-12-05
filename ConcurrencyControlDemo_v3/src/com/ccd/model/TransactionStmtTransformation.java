package com.ccd.model;

import java.util.ArrayList;
import java.util.HashMap;

public class TransactionStmtTransformation {

	ArrayList<String> equation = new ArrayList<String>();

	/**
	 * Finds the type of operation requested by the client, i.e., whether it is
	 * read, write, lock, unlock or an expression.
	 * 
	 * @param ts - input string by client
	 * @return
	 */
	public String operationType(String ts) {

		if (ts.toLowerCase().contains("read") || ts.toLowerCase().contains("r(")) {
			return "read";
		} else if (ts.toLowerCase().contains("write") || ts.toLowerCase().contains("w(")) {
			return "write";
		} else if (ts.toLowerCase().contains("l-s(") || ts.toLowerCase().contains("s(")) {
			return "S_Lock";
		} else if (ts.toLowerCase().contains("l-x(") || ts.toLowerCase().contains("x(")) {
			return "X_Lock";
		} else if (ts.toLowerCase().contains("u(")) {
			return "Unlock";
		} else if (ts.toLowerCase().contains("abort conflicting transaction #")) {
			return "abort conflicting transaction";
		} else if (ts.toLowerCase().contains("abort")) {
			return "abort";
		}

		else {
			return "expression";
		}

	}

	/**
	 * Finds which dataItem is involved in operations like 'read(x)', 'write(y)',
	 * l-x(a), u(a), etc.
	 * 
	 * @param ts            - input string by client
	 * @param operationType - read, write, unlock, S_Lock or X_Lock
	 * @return data-item/element involved
	 */
	public String dataElement(String ts, String operationType) {
		String c = "";
		if (operationType == "read" || operationType == "write" || operationType == "S_Lock"
				|| operationType == "X_Lock" || operationType == "Unlock") {
			int index1 = ts.indexOf("(", 0);
			int index2 = ts.indexOf(")", 0);
			for (int i = index1 + 1; i < index2; i++) {
				c = c + Character.toString(ts.charAt(i));
			}
		}
		return c;
	}
	
	/**
	 * To perform arithmetic operations for equations with more than 2 operands.
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
		double result = 0.0;
		ArrayList<String> operator = new ArrayList<String>();

		// Remove spaces
		ts = ts.replace(" ", "");

		// extract operands x, y and numbers
		String[] operands = ts.split("[=+-/*]");
		// String[] operands = ts.split("\\p{Punct}");

		// Remove any number of digits with just one alphabet 'q', i.e., 100 with 'q' or
		// 1 with 'q'
		// this will help in extracting arithmetic operators with split function without
		// any unwanted empty indices
		ts1 = ts.replaceAll("[0-9]+", "q");

		// extract assignment and arithmetic operators
		String[] operators = ts1.split("[a-zA-Z()]");

		resultArray.add(0, operands[0]);

		// store operators +,-,*,/ from the incoming equation "ts" into ArrayList
		// 'operator'
		if (operands.length > 2) {
			for (int op = 2; op < operators.length; op++) {
				operator.add(operators[op].trim());
			}
		}
		// store values for variables, in the incoming equation "ts", in integer form
		// to perform calculations later
		for (int i = 1; i < operands.length; i++) {
			String operand = null;
			operand = operands[i].trim();
			addOperatorAroundBracketsIFMissing(operand, transTable);

			if (operands.length > 2) {
				if (i < (operands.length - 1)) {
					int op = 0;
					while (operator.size() > 0) {
						if (operator.get(op).contains("/")) {
							equation.add(operator.get(op));
							operator.remove(op);
							operator.trimToSize();
							break;
						} else if (operator.get(op).contains("*")) {
							equation.add(operator.get(op));
							operator.remove(op);
							operator.trimToSize();
							break;
						} else if (operator.get(op).contains("+")) {
							equation.add(operator.get(op));
							operator.remove(op);
							operator.trimToSize();
							break;
						} else if (operator.get(op).contains("-")) {
							equation.add(operator.get(op));
							operator.remove(op);
							operator.trimToSize();
							break;
						} else {
							operator.remove(op);
							operator.trimToSize();
						}
					}
				}
			}
		}

		if (equation.size() == 1) {
			resultArray.add(1, equation.get(0).toString());
			return resultArray;
		}

		// ************************************ performing arithmetic operation
		// according to BODMAS*********************************************/

		// solving bracket
		boolean isBracketThere = false;
		String equationString = equationString(equation);

		while (equationString.contains("(")) {
			isBracketThere = true;
			ArrayList<Integer> StartandEndofBracket = findStartandEndofBracket(equation);
			result = calculationBODMAS(equation, StartandEndofBracket.get(0) + 1, StartandEndofBracket.get(1),
					isBracketThere, result);

			equation.set(StartandEndofBracket.get(0), Double.toString(result));
			while (equation.get(StartandEndofBracket.get(0) + 1) != ")") {
				equation.remove(StartandEndofBracket.get(0) + 1);
				equation.trimToSize();
			}
			equation.remove(StartandEndofBracket.get(0) + 1);
			equation.trimToSize();

			equationString = equationString(equation);
		}
		isBracketThere = false;
		result = calculationBODMAS(equation, 0, (equation.size() - 1), isBracketThere, result);

		resultArray.add(1, Double.toString(result));

		return resultArray;

	}

	private boolean expressionHelper(String operand, HashMap<String, Double> transTable) {
		if (transTable.containsKey(operand)) {
			equation.add(Double.toString(transTable.get(operand)));
		} else if (operand.matches("\\d+")) {
			equation.add(operand);
		} else {
			equation.add(0, "Value for the data item " + operand + "  is missing!");
			return false;
		}
		return true;
	}

	private double calculationBODMAS(ArrayList<String> equation, int start, int end, boolean isBracketThere,
			double result) {

		// division
		for (int k = start; k < end; k++) {
			if (equation.get(k + 1).contains("/")) {
				result = Double.parseDouble(equation.get(k)) / Double.parseDouble(equation.get(k + 2));
				equation.set(k, Double.toString(result));
				end = handleMultipleOccuranceOfOperator(k + 1, equation, isBracketThere);
				k--;
			}
		}

		// multiplication
		for (int k = start; k < end; k++) {
			if (equation.get(k + 1).contains("*")) {
				result = Double.parseDouble(equation.get(k)) * Double.parseDouble(equation.get(k + 2));
				equation.set(k, Double.toString(result));
				end = handleMultipleOccuranceOfOperator(k + 1, equation, isBracketThere);
				k--;
			}
		}

		// addition
		for (int k = start; k < end; k++) {
			if (equation.get(k + 1).contains("+")) {
				result = Double.parseDouble(equation.get(k)) + Double.parseDouble(equation.get(k + 2));
				equation.set(k, Double.toString(result));
				end = handleMultipleOccuranceOfOperator(k + 1, equation, isBracketThere);
				k--;
			}
		}

		// subtraction
		for (int k = start; k < end; k++) {
			if (equation.get(k + 1).contains("-")) {
				result = Double.parseDouble(equation.get(k)) - Double.parseDouble(equation.get(k + 2));
				equation.set(k, Double.toString(result));
				end = handleMultipleOccuranceOfOperator(k + 1, equation, isBracketThere);
				k--;
			}
		}
		return result;
	}

	private String equationString(ArrayList<String> equation) {
		String equationString = "";
		for (int i = 0; i < equation.size(); i++) {
			equationString = equationString + equation.get(i);
		}
		return equationString;
	}

	private ArrayList<Integer> findStartandEndofBracket(ArrayList<String> equation) {
		ArrayList<Integer> indicesofBracket = new ArrayList<Integer>();
		int endOfBracket = 0;
		endOfBracket = equation.indexOf(")");
		indicesofBracket.add(0, equation.lastIndexOf("("));

		if (endOfBracket < indicesofBracket.get(0)) {
			indicesofBracket.add(1, equation.lastIndexOf(")"));
		} else {
			indicesofBracket.add(1, endOfBracket);
		}
		return indicesofBracket;
	}

	private int handleMultipleOccuranceOfOperator(int index, ArrayList<String> equation, boolean isBracketThere) {
		equation.remove(index);
		equation.remove(index);
		equation.trimToSize();
		if (isBracketThere) {
			return findStartandEndofBracket(equation).get(1);
		} else {
			return (equation.size() - 1);
		}
	}

	/**
	 * 
	 * @param operand
	 * @param transTable
	 */
	private void addOperatorAroundBracketsIFMissing(String operand, HashMap<String, Double> transTable) {

		if (operand.contains("(") && !operand.contains(")")) {
			int j = 0;
			while (j < operand.length()) {
				String opr = "";

				while (operand.charAt(j) == '(') {
					equation.add("(");
					j++;
				}
				while (j < operand.length()) {
					if (operand.charAt(j) != '(') {
						opr = opr + operand.charAt(j);
					} else if (operand.charAt(j) == '(') {
						break;
					}
					j++;
				}
				if (opr != "") {
					expressionHelper(opr, transTable);
				}
				if (j != operand.length()) {
					equation.add("*");
				}
			}
		} else if (operand.contains(")") && !operand.contains("(")) {
			int j = 0;
			int indexofB = 0;
			while (j < operand.length()) {
				String opr = "";

				while (j < operand.length()) {
					if (operand.charAt(j) != ')') {
						opr = opr + operand.charAt(j);
					} else if (operand.charAt(j) == ')') {
						break;
					}
					j++;

				}
				if (opr != "") {
					expressionHelper(opr, transTable);
				}
				for (int i = indexofB + 1; i < operand.length(); i++) {
					if (operand.charAt(i) == ')') {
						equation.add(")");
						j = i;
						indexofB = i;
					}
				}
				if (j < operand.length() - 1) {
					equation.add("*");
				}
				j++;
			}
		} else if (operand.contains("(") && operand.contains(")")) {
			int j = 0;
			while (j < operand.length()) {
				String opr = "";
				if (j == 0 && operand.charAt(j) == '(') {
					j++;
				}
				while (j < operand.length()) {
					if ((operand.charAt(j) != '(' && operand.charAt(j) != ')')) {
						opr = opr + operand.charAt(j);
					} else {
						break;
					}
					j++;
				}
				if (opr != "") {
					expressionHelper(opr, transTable);
				}
				if (j < operand.length() && operand.charAt(j) != ')') {
					equation.add("*");
				}
				j++;
			}

		} else {
			expressionHelper(operand, transTable);
		}

	}
}
