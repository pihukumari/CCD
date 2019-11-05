<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="ISO-8859-1">
<title>Concurrency Control Demo</title>
</head>
<body>
<%!
static String oldTranID;
%>
<%
oldTranID = request.getAttribute("oldTranID").toString(); 

%>



	<b>Select the Concurrency Control Algorithm:</b>
	<br />
	<form action="controllerV2" method="post">
		<select name="algorithm">
			<option value="Two_Phase_Locking">Two-Phase Locking</option>
			<option value="Timestamp_Ordering">Timestamp Ordering</option>
			<option value="Optimistic_Concurrency_Control">Optimistic
				Concurrency Control</option>
			<option value="Snapshot_Isolation">Snapshot Isolation</option>
			<option value=<%=session.getAttribute("algo")%> selected disabled
				style="color: rgb(255, 255, 255);"></option>
		</select> &nbsp; <input type="submit" value="Select" /> &nbsp; &nbsp;
		<%=session.getAttribute("algo")%>
		<br /> <br />
	</form>

	<b>Enter the transaction steps here:</b>
	<br />
	<br />
	<form action="controllerV2" method="post">
		<input type="text" name="ts1" placeholder="e.g., read(x)" /> &nbsp; <input
			type="submit" value="execute" /> &nbsp;
		<%=session.getAttribute("t1")%>
		<br />
		<%=session.getAttribute("commit"+oldTranID) %>
		
	</form>
	<br />
	<form action="controllerV2" method="post">
		<input type="text" name="ts2" />&nbsp; <input type="submit"
			value="execute" /> &nbsp;
		<%=session.getAttribute("t2")%>
		<br />
	</form>
	<br />
	<form action="controllerV2" method="post">
		<input type="text" name="ts3" /> &nbsp; <input type="submit"
			value="execute" /> &nbsp;
		<%=session.getAttribute("t3")%>
		<br />
	</form>
	<br />
	<form action="controllerV2" method="post">
		<input type="text" name="ts4" /> &nbsp; <input type="submit"
			value="execute" /> &nbsp;
		<%=session.getAttribute("t4")%>
		<br />
	</form>
	<br />
	<form action="controllerV2" method="post">
		<input type="text" name="ts5" />&nbsp; <input type="submit"
			value="execute" /> &nbsp;
		<%=session.getAttribute("t5")%>
		<br />
	</form>
	<br />
	<form action="controllerV2" method="post">
		<input type="text" name="ts6" /> &nbsp; <input type="submit"
			value="execute" /> &nbsp;
		<%=session.getAttribute("t6")%>
		<br />
	</form>
	<br />
	<form action="controllerV2" method="post">
		<input type="text" name="ts7" /> &nbsp; <input type="submit"
			value="execute" /> &nbsp;
		<%=session.getAttribute("t7")%>
		<br />
	</form>
	<br />
	<form action="controllerV2" method="post">
		<input type="text" name="ts8" /> &nbsp; <input type="submit"
			value="execute" /> &nbsp;
		<%=session.getAttribute("t8")%>
		<br />
	</form>
	<form action="controllerV2" method="get">
		<input type="submit" value="commit" /> <br />
	</form>

</body>
</html>