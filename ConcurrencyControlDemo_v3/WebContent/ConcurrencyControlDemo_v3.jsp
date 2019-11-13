<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1" import="java.util.*" %>
<!DOCTYPE html>
<html>
<head>
<meta charset="ISO-8859-1">
<title>Concurrency Control Demo</title>
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

	<b>Select the Concurrency Control Algorithm:</b>
	<br />
	<form action="controllerV2" method="post">
		<select name="algorithm">
			<option value="Two-Phase Locking">Two-Phase Locking</option>
			<option value="Timestamp Ordering">Timestamp Ordering</option>
			<option value="Optimistic Concurrency Control">Optimistic Concurrency Control</option>
			<option value="Snapshot Isolation">Snapshot Isolation</option>
			<option value=<%=session.getAttribute("algo")%> selected disabled
				style="color: rgb(255, 255, 255);"></option>
		</select> &nbsp; 
		<input type="submit" value="Select" /> &nbsp; &nbsp;
		<% if (session.getAttribute("algo") != null) { %>
			<%=session.getAttribute("algo")%>
		<% } %>
		<br /> <br />
	</form>
		<% if (session.getAttribute("tranID") != null) { %>
		<i> Current Transaction ID: # <%=session.getAttribute("tranID")%>  </i><br /><br />
		<% } %>

	<b>Enter the transaction steps here:</b>
	<br />
	<br />
	<form action="controllerV2" method="post">
		<input type="text" name="ts" placeholder="e.g., read(x)" /> &nbsp; 
		<input type="submit" value="execute" /> &nbsp;
		
	</form>
		<% for (int i = 1; i <= counter; i++){ 
				if (session.getAttribute("t" + Integer.toString(i)) != null) { %>
						<%=session.getAttribute("t" + Integer.toString(i)) %><br />
				<%}
			} %>

	<br />
	
	<form action="controllerV2" method="post">
		<input type="hidden" name="commit" value="yes" />
		<input type="submit" value="commit" />
	</form>
	
	 <br />
		

</body>
</html>