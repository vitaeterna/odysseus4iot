# Welcome to GlobalQueryScript(GQS) for Odysseus

## What is GlobalQueryScript(GQS)
GlobalQueryScript(GQS) is a Java application which enables the deployment of global queries to [Odysseus](https://odysseus.informatik.uni-oldenburg.de/) nodes. A global query is defined in JSON format. A sample global query definition can be found [here](https://gitlab.rz.uni-bamberg.de/mobi/futureiot/odysseus4iot/-/blob/master/eclipse-project/odysseus4iot/globalquery1.json).  

A global query consists of a **name** and a **list of partial queries**. A partial query represents a typical query in a DSMS like Odysseus. It has a **name** and a **parser** which defines the language in which the query is written. Also **server** information is provided which holds the **socket** and **user credentials** of the Odysseus node where this query is thought to be installed to. Lastly, the **queryText** contains the actual query. A sample Odysseus query can be found [here](https://gitlab.rz.uni-bamberg.de/mobi/futureiot/odysseus4iot/-/blob/master/eclipse-project/odysseus4iot/samplequery.qry). In order to convert a pretty formatted query text to a single line JSON value string you can use a tool like [Unicode to Java string literal converter](http://snible.org/java2/uni2java.html).  

GlobalQueryScript(GQS) was developed using [OpenJDK 11](https://jdk.java.net/java-se-ri/11) and [Gson](https://mvnrepository.com/artifact/com.google.code.gson/gson).

## How to use GlobalQueryScript(GQS)
You can either get the source code by cloning this repository and compiling and running it on your own or by using the [executable jar file](https://gitlab.rz.uni-bamberg.de/mobi/futureiot/odysseus4iot/-/blob/master/eclipse-project/odysseus4iot/gqs.jar). There are two modes in which GlobalQueryScript(GQS) can be used:
1. **Interactive Mode**  
When running GQS without parameters you will start it in interactive mode. This basically is a command line tool, where you can interactively put commands.
   ```
   java -jar gqs.jar
   ```
1. **Script Mode**  
When running GQS with {file} parameter you will start it in script mode. In this mode all the commands which are defined in the provided GQS script will be executed in order. A sanple GQS script can be found [here](https://gitlab.rz.uni-bamberg.de/mobi/futureiot/odysseus4iot/-/blob/master/eclipse-project/odysseus4iot/script1.gqs).
   ```
   java -jar gqs.jar script.gqs
   ```
## Available Commands
The commands which are available in the interactive mode and can be used writing a GQS schript are depicted in the following table.  

| Command          | Description                            |
| ---------------- |  ------------------------------------- |
| load {file}      | loads a global query from a json file  |
| unload {qname}   | unloads a global query by query name   |
| list             | lists all loaded global queries        |
| deploy {qname}   | deploys a global query by query name   |
| undeploy {qname} | undeploys a global query by query name |
| exit             | terminates the application             |
| quit             | terminates the application             |