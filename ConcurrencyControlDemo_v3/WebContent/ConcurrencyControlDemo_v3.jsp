<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1" import="java.util.*" %>
<!DOCTYPE html>
<html>
<head>
<title>Concurrency Control Demonstrator</title><head>
<meta charset="ISO-8859-1">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
* 
body {
  font-family: Arial, Helvetica, sans-serif;
  background-color: #f9feff;
}
h1 {
  color: #008080;
}

</style>
</head>
<body>
<%!
int numberOfTran;
int counter;
ArrayList<String> a = new ArrayList<String>();
%>
<%
if (request.getAttribute("counter") != null) {
	counter = Integer.parseInt(request.getAttribute("counter").toString());
}

%>
<h1>Concurrency Control Demonstrator</h1>
	<b>Select the Concurrency Control Algorithm:</b>
	<br /><br />
	<form action="controllerV2" method="post">
		<select name="algorithm">
			<option value="Two-Phase Locking">Two-Phase Locking</option>
			<option value="Timestamp Ordering">Timestamp Ordering</option>
			<option value="Optimistic Concurrency Control BOCC">Optimistic Concurrency Control BOCC</option>
			<option value="Optimistic Concurrency Control FOCC">Optimistic Concurrency Control FOCC</option>
			<!-- <option value="Snapshot Isolation">Snapshot Isolation</option>  -->
			<option value=<%=session.getAttribute("algo")%> selected disabled
				style="color: rgb(255, 255, 255);"></option>
		</select> &nbsp;&nbsp; 
		
		<% if (session.getAttribute("algo") != null) { %>
			<%=session.getAttribute("algo")%> <br />
		<% } %>
		<br /> <br />
		<% if (session.getAttribute("tranID") != null) { %>
		<i> Current Transaction ID: # <%=session.getAttribute("tranID")%>  </i><br /><br />
		<% } if (session.getAttribute("timestamp") != null) { %>
			<i> Start Time: </i> <%=session.getAttribute("timestamp")%> <br/><br />
		<% } %>

	<b>Enter the transaction steps here:</b>
	<br /><br />
		<input type="text" name="ts" placeholder="e.g., read(x) or r(x)" /> &nbsp; 
		<input type="submit" value="execute" /> &nbsp;
		
	</form>
	
		<% for (int i = 1; i <= counter; i++){ 
				if (session.getAttribute("t" + Integer.toString(i)) != null) { %>
						<%=session.getAttribute("t" + Integer.toString(i)) %><br />
				<%
				}
			} %>

	<br />
	

		

</body>
</html>