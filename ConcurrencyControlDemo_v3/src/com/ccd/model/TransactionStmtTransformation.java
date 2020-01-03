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

		// will store data item and its value, e.g., x and value of x
		ArrayList<String> resultArray = new ArrayList<String>();

		// check if the input 'ts' is actually an expression or not
		if (!ts.contains("=")) {
			// if not an expression
			resultArray.add("Invalid input");

		} else {

			String ts1 = null;

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

			// add the data item for which the expression is - eg.- for x = 5*6 -> store x
			// here
			resultArray.add(0, operands[0]);

			// store operators +,-,*,/ from the incoming equation "ts" into ArrayList
			// 'operator'
			if (operands.length > 2) {
				for (int op = 2; op < operators.length; op++) {
					operator.add(operators[op].trim());
				}
			}
			// store values for dataitems, in the incoming equation "ts", in integer form
			// to perform calculations later
			for (int i = 1; i < operands.length; i++) {
				String operand = null;
				operand = operands[i].trim();
				// add * operator before brackets if it is missing - eg.- x = 2(5+4) -> make it
				// x = 2*(5+4)
				addOperatorAroundBracketsIFMissing(operand, transTable);

				// add operators to the equation arraylist the operators array extracted from
				// input string ts
				if (operands.length > 2) {
					if (i < (operands.length - 1)) {
						int op = 0;
						while (operator.size() > 0) {
							if (operator.get(op).contains("/")) {
								equation.add(operator.get(op));
								// remove the operator from the array in each if condition
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

			// this handles single value equations like x=5. It adds 5 here
			if (equation.size() == 1) {
				resultArray.add(1, equation.get(0).toString());
				return resultArray; // the method will exit here and output is sent to calling class

			} else if (equation.get(0).contains("Value for the data item ")) {
				// this is to handle the case when a data item is used in the equation but is
				// not read by the transaction yet
				resultArray.clear();
				resultArray.add(0, equation.get(0).toString());
				return resultArray; // the method will exit here and output is sent to calling class
			}

//********** performing arithmetic operation according to BODMAS**************/

			// solving bracket

			// set this to false for now
			boolean isBracketThere = false;

			// this is used to check if there are any brackets in the equation
			String equationString = equationString(equation);

			// after each execution of this loop, the result of the two adjacent operand is
			// calculated and stored back into the equation arraylist. That means equation
			// keeps reducing in size until it reaches size=1, and this one number is the
			// value of the expression came in 'ts'

			while (equationString.contains("(")) {
				isBracketThere = true; // set to true

				ArrayList<Integer> StartandEndofBracket = findStartandEndofBracket(equation);

				try {
					// 'result' contains the solution of the expression inside the bracket
					result = calculationBODMAS(equation, StartandEndofBracket.get(0) + 1, StartandEndofBracket.get(1),
							isBracketThere, result);
				} catch (Exception e) {
					resultArray.clear();
					resultArray.add(
							"Please add arithmatic operators to the equations wherever it is missing! </br> And remove brackets if not necessary for calculations");
					return resultArray;
				}
				// replace the result into the arraylist equation and remove the elements of the
				// solved bracket.
				equation.set(StartandEndofBracket.get(0), Double.toString(result));
				while (equation.get(StartandEndofBracket.get(0) + 1) != ")") {
					equation.remove(StartandEndofBracket.get(0) + 1);
					equation.trimToSize();
				}
				equation.remove(StartandEndofBracket.get(0) + 1);
				equation.trimToSize();

				// use the modified equation arraylist to set equationString, so that this while
				// loop can check and handle more brackets, if any
				equationString = equationString(equation);
			}

			// set to false as no more brackets left to solve
			isBracketThere = false;

			// solve the remaining equation
			try {
				result = calculationBODMAS(equation, 0, (equation.size() - 1), isBracketThere, result);
			} catch (Exception e) {
				resultArray.clear();
				resultArray.add(
						"Please add arithmatic operators to the equations wherever it is missing! </br> And remove brackets if not necessary for calculations");
				return resultArray;
			}

			// add the result to the return arraylist
			resultArray.add(1, Double.toString(result));
		}
		return resultArray;

	}

	/**
	 * Identifies the operands - whether it is a dataitem or a number. If it is data
	 * item, the check if it is in transTable or not,i.e., it has been read by the
	 * transaction or not. If not, message about missing data item is sent to the
	 * calling method.
	 * 
	 * @param operand
	 * @param transTable
	 * @return
	 */
	private boolean expressionHelper(String operand, HashMap<String, Double> transTable) {
		if (transTable.containsKey(operand)) {
			// when operand is a data item - fetch its value from transTable and add to
			// equation array
			equation.add(Double.toString(transTable.get(operand)));
		} else if (operand.matches("\\d+")) {
			// when operand is a number - add directly
			equation.add(operand);
		} else {
			// this is to handle the case when a data item is used in the equation but is
			// not read by the transaction yet
			equation.add(0, "Value for the data item " + operand + "  is missing!");
			return false;
		}
		return true;
	}

	/**
	 * Perform BODMAS calculation on the equation.
	 * 
	 * @param equation
	 * @param start
	 * @param end
	 * @param isBracketThere
	 * @param result
	 * @return
	 */
	private double calculationBODMAS(ArrayList<String> equation, int start, int end, boolean isBracketThere,
			double result) throws Exception {

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

	/**
	 * Converts equation arraylist into a string.
	 * 
	 * @param equation
	 * @return
	 */
	private String equationString(ArrayList<String> equation) {
		String equationString = "";
		for (int i = 0; i < equation.size(); i++) {
			equationString = equationString + equation.get(i);
		}
		return equationString;
	}

	/**
	 * Finds the index of '(' and ')' in the equation from 'ts'
	 * 
	 * @param equation
	 * @return Integer Arraylist containing the indices
	 */
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

	/**
	 * It is only called by method calcultionBODMAS to handle the multiple
	 * occurrences of each operations.
	 * 
	 * @param index
	 * @param equation
	 * @param isBracketThere
	 * @return
	 */
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
	 * Adds multiplication operator before the brackets if it is missing. It handles
	 * one element of the operands array at a time. operands array is generated by
	 * splitting 'ts' with [=+-*\/] as the delimiters.
	 * 
	 * @param operand
	 * @param transTable
	 */
	private void addOperatorAroundBracketsIFMissing(String operand, HashMap<String, Double> transTable) {

		// to handle operands' elements like 2(10 that only have open bracket(s)
		if (operand.contains("(") && !operand.contains(")")) {

			int j = 0;
			// loop through the length of element
			while (j < operand.length()) {
				String opr = "";

				// when bracket encountered (any number of times), add to the equation
				while (operand.charAt(j) == '(') {
					equation.add("(");
					j++;
				}

				// extract the numbers or data item before or after the bracket
				while (j < operand.length()) {
					if (operand.charAt(j) != '(') {
						opr = opr + operand.charAt(j);
					} else {
						break;
					}
					j++;
				}

				// add the extracted number or data item to the equation
				if (opr != "") {
					expressionHelper(opr, transTable);
				}

				// add * to the equation if the loop is not over yet
				if (j != operand.length()) {
					equation.add("*");
				}
			}
		} else if (operand.contains(")") && !operand.contains("(")) {
			// to handle operands' elements like 2)10 - that only have close bracket(s)

			int j = 0;
			int indexofB = 0; // index of close bracket

			while (j < operand.length()) {
				String opr = "";

				while (j < operand.length()) {
					if (operand.charAt(j) != ')') {
						opr = opr + operand.charAt(j);
					} else {
						break;
					}
					j++;

				}
				if (opr != "") {
					expressionHelper(opr, transTable);
				}

				// add close bracket(s) to the equation arraylist.
				for (int i = indexofB + 1; i < operand.length(); i++) {
					if (operand.charAt(i) == ')') {
						equation.add(")");
						j = i;
						indexofB = i;
					}
				}

				// add * after the ')' to the equation arraylist only if there is a character or
				// number after ')'
				if (j < operand.length() - 1) {
					equation.add("*");
				}
				j++;
			}
		} else if (operand.contains("(") && operand.contains(")")) {
			// to handle operands' elements like 2)(100 or (100) or 2(10), etc.

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

				if (j < operand.length()) {

					// add * to the equation if the loop is not over yet
					if (operand.charAt(j) != ')') {
						equation.add("*");
					} else if (operand.charAt(j) == ')') {
						// add the close bracket
						equation.add(")");
					}
				}

				// add the close bracket to the equation
				if (j < operand.length() && operand.charAt(j) == '(') {
					equation.add("(");
				}
				j++;
			}

		} else {
			// when operands' elements has no brackets
			expressionHelper(operand, transTable);
		}

	}
}
