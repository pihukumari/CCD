# Concurrency Control Demonstrator

It is a web application that demonstrates the working principle of concurrency control algorithms - two phase locking, timestamp ordering, backward- and forward-oriented optimistic concurrency control algorithms. It also demonstrates cascade abort when a transaction aborts for any reason, except in case of system failure.

## Prerequisites
- Install the latest jdk on eclipse.
- Download Apache Tomcat 8.5 from the link: https://tomcat.apache.org/download-80.cgi. Download from the repective zip file links - eg., for Mac or Windows, etc.
- Unzip the downloaded file in any folder you want.
- Go to Eclipse, open Servers view from Window -> Show View -> Servers.
- Click the link on the Servers view to create a new server.
- A window will open. Select Tomcat8.5 under Apache folder and click next.
- Browse the Tomcat Installation directry and go to the folder where you had unziped the Tomcat zip file earlier. Select the home folder - it's name would be something like 'apache-tomcat-8.5.xx'. Click next and then click finish.

### Installing
- Clone the git repository to the eclipse from the link: https://github.com/pihukumari/CCD.git
- Switch to JavaEE perspective if not already in it. To do this Go to Window -> Perspective -> Open Perspective -> JavaEE
- Go to Window -> Web Browser -> Select any of the brower you want but not the internal web browser.

## Getting Started
- In the eclipse, inside the project folder, go to WebContent folder.
- Right click on the ConcurrencyControDemo_v3.jsp file and choose Run -> Run on Server. A session will open in the browser you chose in the above section.
- Copy the link and paste it to other browser(s) to start different sessions of the application.
- Now you are ready to run any operations. The allowed operations are as follows:
  - To read data item *x*, enter *read(x)* or *r(x)* and click execute. Make sure to select an algorithm from the drop-down list.
  - To write data item *x*, enter *write(x)* or *w(x)*.
  - To assign a value to data item before executing write operation, enter *x=10*.
  - For two-phase locking (2PL) protocol, you can explicitly request share lock of *x* with the command *l-s(x)* or *s(x)* and exclusive lock with *l-x(x)* or *x(x)*
  - unlock a data item with *unlock(x)* or *u(x)*.
  - Commit the transaction with command *commit*.
  - Abort with the command *abort*.
  - Note that for 2PL, when read and write commands are executed, appropriate locks are acquired automatically. U can use locking commands when you know that you are going to read and write a data item and hence you acquire the exclusive lock directly.
