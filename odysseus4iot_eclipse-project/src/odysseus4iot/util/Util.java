package odysseus4iot.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import odysseus4iot.deployment.store.model.GlobalQuery;
import odysseus4iot.deployment.store.model.PartialQuery;
import odysseus4iot.deployment.store.model.Server;
import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Vertex;
import odysseus4iot.graph.operator.meta.DataFlow;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.graph.operator.meta.OperatorGraph;
import odysseus4iot.graph.physical.meta.Connection;
import odysseus4iot.graph.physical.meta.Node;
import odysseus4iot.graph.physical.meta.PhysicalGraph;
import odysseus4iot.graph.physical.meta.Node.Type;
import odysseus4iot.main.Main;

public class Util
{
	public static String readFile(String path, Charset encoding)
    {
        byte[] encoded = null;

        try
        {
            encoded = Files.readAllBytes(Paths.get(path));
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }

        return new String(encoded, encoding);
    }

    public static void writeFile(String path, String content, Charset encoding)
    {
        byte[] encoded = content.getBytes(encoding);

        try
        {
            Files.write(new File(path).toPath(), encoded);
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
    }
    
    public static boolean deleteDirectory(File dir)
    {
        if(dir.exists())
        {
            if(dir.isDirectory())
            {
                String[] children = dir.list();

                for(int i = 0; i < children.length; i++)
                {
                    if(!deleteDirectory(new File(dir, children[i])))
                    {
                        return false;
                    }
                }
            }

            return dir.delete();
        }
        else
        {
            return true;
        }
    }

    public static boolean createDirectory(File dir)
    {
        return dir.mkdirs();
    }
	
    /**
     * 
     * @param sizeInBits in bits
     * @return
     */
    public static String formatSizeInBits(Double sizeInBits)
    {
    	int unitSize = 1000;
    	
    	if(sizeInBits < unitSize)
    	{
    		return String.format("%.3f Bit", sizeInBits);
    	}
    	
        int unitIndex = (int) (Math.log10(sizeInBits) / Math.log10(unitSize));
        Character unit = "KMGT".charAt(unitIndex - 1);
        
        return String.format("%.3f %sBit", sizeInBits / Math.pow(unitSize, unitIndex), unit);
    }
    
    /**
     * From https://stackoverflow.com/questions/6710094/how-to-format-an-elapsed-time-interval-in-hhmmss-sss-format-in-java
     * 
     * @param timestamp in ms (milliseconds)
     * @return
     */
    public static String formatTimestamp(Long timestamp)
    {
    	final long d = TimeUnit.MILLISECONDS.toDays(timestamp);
        final long h = TimeUnit.MILLISECONDS.toHours(timestamp - TimeUnit.DAYS.toMillis(d));
        final long min = TimeUnit.MILLISECONDS.toMinutes(timestamp - TimeUnit.DAYS.toMillis(d) - TimeUnit.HOURS.toMillis(h));
        final long s = TimeUnit.MILLISECONDS.toSeconds(timestamp - TimeUnit.DAYS.toMillis(d) - TimeUnit.HOURS.toMillis(h) - TimeUnit.MINUTES.toMillis(min));
        final long ms = TimeUnit.MILLISECONDS.toMillis(timestamp - TimeUnit.DAYS.toMillis(d) - TimeUnit.HOURS.toMillis(h) - TimeUnit.MINUTES.toMillis(min) - TimeUnit.SECONDS.toMillis(s));
        
        if(d == 0 && h == 0 && min == 0 && s == 0)
        {
        	return String.format("%d milliseconds", ms);
        }
        
        if(d == 0 && h == 0 && min == 0)
        {
        	return String.format("%d seconds %d milliseconds", s, ms);
        }
        
        if(d == 0 && h == 0)
        {
        	return String.format("%d minutes %d seconds %d milliseconds", min, s, ms);
        }
        
        if(d == 0)
        {
        	return String.format("%d hours %d minutes %d seconds %d milliseconds", h, min, s, ms);
        }
        
        return String.format("%d days %d hours %d minutes %d seconds %d milliseconds", d, h, min, s, ms);
        
        //return String.format("%02d:%02d:%02d.%03d", hr, min, sec, ms);
    }
    
	public static void validateProperties()
	{
		List<String> requiredProperties = new ArrayList<>();
		requiredProperties.add("input.sensors");
		requiredProperties.add("input.nodenames");
		requiredProperties.add("input.nodesockets");
		requiredProperties.add("input.nodetypes");
		requiredProperties.add("input.nodecpucaps");
		requiredProperties.add("input.nodememcaps");
		requiredProperties.add("input.edges");
		requiredProperties.add("input.edgeratecaps");
		requiredProperties.add("input.edgedelays");
		requiredProperties.add("input.ids");
		requiredProperties.add("pythonrpc.sockets");
		requiredProperties.add("sensordb.url");
		requiredProperties.add("sensordb.user");
		requiredProperties.add("sensordb.password");
		requiredProperties.add("modeldb.host");
		requiredProperties.add("modeldb.port");
		requiredProperties.add("modeldb.database");
		requiredProperties.add("modeldb.table");
		requiredProperties.add("modeldb.column");
		requiredProperties.add("modeldb.user");
		requiredProperties.add("modeldb.password");
		requiredProperties.add("predictiondb.url");
		requiredProperties.add("predictiondb.user");
		requiredProperties.add("predictiondb.password");
		
		String currentProperty = null;
		String currentValue = null;
		
		for(int index = 0; index < requiredProperties.size(); index++)
		{
			currentProperty = requiredProperties.get(index);
			
			currentValue = Main.properties.getProperty(currentProperty);
			
			if(currentValue == null)
			{
				System.err.println("The required property '" + currentProperty + "' could not be found.");
				
				System.exit(0);
			}
		}
		
		int idCount = Main.properties.getProperty("input.ids").split(",").length;
		int rpcCount = Main.properties.getProperty("pythonrpc.sockets").split(",").length;
		
		if(idCount != rpcCount)
		{
			System.err.println("The length of the arrays 'input.ids' and 'pythonrpc.sockets' needs to be equal.");
			
			System.exit(0);
		}
		
		int nodeNameCount = Main.properties.getProperty("input.nodenames").split(",").length;
		int nodeSocketCount = Main.properties.getProperty("input.nodesockets").split(",").length;
		int nodeTypeCount = Main.properties.getProperty("input.nodetypes").split(",").length;
		int nodeCPUCapCount = Main.properties.getProperty("input.nodecpucaps").split(",").length;
		int nodeMemCapCount = Main.properties.getProperty("input.nodememcaps").split(",").length;
		
		if(nodeNameCount != nodeSocketCount || nodeNameCount != nodeTypeCount || nodeNameCount != nodeCPUCapCount || nodeNameCount != nodeMemCapCount)
		{
			System.err.println("The length of the arrays 'input.nodenames', 'input.nodeSockets', 'input.nodetypes', 'input.nodecpucaps' and 'input.nodememcaps' needs to be equal.");
			
			System.exit(0);
		}
		
		int edgeCount = Main.properties.getProperty("input.edges").split(",").length;
		int edgeRateCount = Main.properties.getProperty("input.edgeratecaps").split(",").length;
		int edgeDelayCount = Main.properties.getProperty("input.edgedelays").split(",").length;
		
		if(edgeCount != edgeRateCount || edgeCount != edgeDelayCount)
		{
			System.err.println("The length of the arrays 'input.edges', 'input.edgeratecaps' and 'input.edgedelays'needs to be equal.");
			
			System.exit(0);
		}
	}
	
    public static String toJson(Object object)
    {
        return (new GsonBuilder().setPrettyPrinting().create()).toJson(object);
    }

    public static <T> T fromJson(String json, Class<T> classT)
    {
        return (new Gson()).fromJson(json, classT);
    }

    public static void charsetUTF8()
    {
        if(!Charset.defaultCharset().name().equals("UTF-8"))
        {
            System.err.print("Default charset is not UTF-8 but " + Charset.defaultCharset().name() + "\n");
            System.err.print("Use VM argument -Dfile.encoding=UTF-8\n");

            System.exit(0);
        }
        else
        {
            System.out.print("Charset: " + Charset.defaultCharset().name() + "\n\n");
        }
    }
	
	public static String exportPQL(OperatorGraph operatorGraph)
	{
		StringBuilder stringBuilder = new StringBuilder();
		
		stringBuilder.append("#PARSER PQL\r\n\r\n");
		stringBuilder.append("#DOQUERYSHARING false\r\n\r\n");
		stringBuilder.append("#METADATA TimeInterval\r\n");
		stringBuilder.append("#METADATA Datarate\r\n");
		stringBuilder.append("#METADATA Latency\r\n\r\n");
		stringBuilder.append("#REQUIRED de.uniol.inf.is.odysseus.database.feature.feature.group false\r\n\r\n");
		stringBuilder.append("#RUNQUERY\r\n\r\n");
		
		List<Vertex> vertices = operatorGraph.getVerticesBreadthFirst(null);
		
		Vertex currentVertex = null;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentVertex = vertices.get(index);
			
			stringBuilder.append(currentVertex.toString());
			
			if(index!=vertices.size()-1)
			{
				stringBuilder.append("\r\n\r\n");
			}
		}
		
		Util.writeFile("./generated/" + operatorGraph.label + ".qry", stringBuilder.toString(), Charset.defaultCharset());
		
		System.out.print("Written to " + "./generated/" + operatorGraph.label + ".qry\n");
		
		return stringBuilder.toString();
	}
    
    public static void exportOperatorGraphToDOTPNG(OperatorGraph operatorGraph)
    {
        StringBuilder dot = new StringBuilder();

        dot.append("digraph OG\n");
        dot.append("{\n");
        dot.append("    graph [outputorder=edgesfirst, splines=true, dpi=200, fontname=\"Courier New Bold\"];\n\n");
        dot.append("    node [style=filled, fillcolor=white, color=black, fontname=\"Courier New Bold\"];\n");

        Operator currentOperator = null;
        
        for(int index = 0; index < operatorGraph.vertices.size(); index++)
        {
        	currentOperator = (Operator)operatorGraph.vertices.get(index);
        	
        	if(currentOperator.type.equals(Operator.Type.SOURCE))
        	{
        		dot.append("    " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=circle, width=1];\n");
        	}
        	else if(currentOperator.type.equals(Operator.Type.SINK))
        	{
        		dot.append("    " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=doublecircle, width=1];\n");
        	}
        	else if(currentOperator.type.equals(Operator.Type.MERGE))
        	{
        		dot.append("    " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=invtriangle, width=3];\n");
        	}
        	else if(currentOperator.type.equals(Operator.Type.PROJECT))
        	{
        		dot.append("    " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=tab, width=2];\n");
        	}
        	else if(currentOperator.type.equals(Operator.Type.PROCESSING))
        	{
        		dot.append("    " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=box, width=2];\n");
        	}
        	else if(currentOperator.type.equals(Operator.Type.BENCHMARK))
        	{
        		dot.append("    " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=note, width=2];\n");
        	}
        	else if(currentOperator.type.equals(Operator.Type.NOP))
        	{
        		dot.append("    " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=Msquare, width=2];\n");
        	}
        	else
        	{
        		System.err.println("No routine for Operator.Type." + currentOperator.type + " implemented.");
        		
        		System.exit(0);
        	}
        }

        dot.append("\n    edge [arrowhead=vee, arrowtail=none, color=black, fontname=\"Courier New Bold\", weight=1];\n");

        Edge currentEdge = null;
        
        for(int index = 0; index < operatorGraph.edges.size(); index++)
        {
        	currentEdge = operatorGraph.edges.get(index);
        	
        	dot.append("    " + currentEdge.vertex0.id + ":s -> " + currentEdge.vertex1.id + ":n [label=\"" + currentEdge.label + "\"];\n");
        }

        dot.append("}");

        Util.writeFile("./generated/" + operatorGraph.label + ".dot", dot.toString(), Charset.defaultCharset());
        
        System.out.print("Written to " + "./generated/" + operatorGraph.label + ".dot\n");
        
        //dot -Tpng outputFilename.dot -o outputFilename.png
        ProcessBuilder builder = new ProcessBuilder("dot", "-Tpng", "./generated/" + operatorGraph.label + ".dot", "-o", "./generated/" + operatorGraph.label + ".png");
        builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);

        try
        {
            builder.start().waitFor();

            System.out.print("Written to " + "./generated/" + operatorGraph.label + ".png\n");
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();

            System.exit(0);
        }
        catch(IOException e)
        {
            e.printStackTrace();

            System.out.print("\nPlease check whether you have installed a dot renderer like GraphViz (http://www.graphviz.org/) and if the binaries are available via your PATH variable.\n");
            System.out.print("The generated dot file was not rendered to png.\n");
        }
    }
    
    public static void exportPhysicalGraphToDOTPNG(PhysicalGraph physicalGraph)
    {
        StringBuilder dot = new StringBuilder();

        dot.append("digraph PG\n");
        dot.append("{\n");
        dot.append("    graph [outputorder=edgesfirst, splines=true, dpi=200, fontname=\"Courier New Bold\"];\n\n");
        dot.append("    node [style=filled, fillcolor=white, color=black, fontname=\"Courier New Bold\"];\n");

        Node currentNode = null;
        
        for(int index = 0; index < physicalGraph.vertices.size(); index++)
        {
        	currentNode = (Node)physicalGraph.vertices.get(index);
        	
        	if(currentNode.type.equals(Node.Type.EDGE))
        	{
        		dot.append("    " + currentNode.id + " [group=g" + currentNode.group + ", label=\"" + currentNode.label + "\", shape=octagon, width=1];\n");
        	}
        	else if(currentNode.type.equals(Node.Type.FOG))
        	{
        		dot.append("    " + currentNode.id + " [group=g" + currentNode.group + ", label=\"" + currentNode.label + "\", shape=doubleoctagon, width=1];\n");
        	}
        	else if(currentNode.type.equals(Node.Type.CLOUD))
        	{
        		dot.append("    " + currentNode.id + " [group=g" + currentNode.group + ", label=\"" + currentNode.label + "\", shape=tripleoctagon, width=1];\n");
        	}
        	else
        	{
        		System.err.println("No routine for Node.Type." + currentNode.type + " implemented.");
        		
        		System.exit(0);
        	}
        }

        dot.append("\n    edge [arrowhead=vee, arrowtail=none, color=black, fontname=\"Courier New Bold\", weight=1];\n");

        Connection currentConnection = null;
        
        for(int index = 0; index < physicalGraph.edges.size(); index++)
        {
        	currentConnection = (Connection)physicalGraph.edges.get(index);
        	
        	dot.append("    " + currentConnection.vertex0.id + ":s -> " + currentConnection.vertex1.id + ":n [label=\"" + currentConnection.label + "\"];\n");
        }

        dot.append("}");

        Util.writeFile("./generated/" + physicalGraph.label + ".dot", dot.toString(), Charset.defaultCharset());
        
        System.out.print("Written to " + "./generated/" + physicalGraph.label + ".dot\n");
        
        //dot -Tpng outputFilename.dot -o outputFilename.png
        ProcessBuilder builder = new ProcessBuilder("dot", "-Tpng", "./generated/" + physicalGraph.label + ".dot", "-o", "./generated/" + physicalGraph.label + ".png");
        builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);

        try
        {
            builder.start().waitFor();

            System.out.print("Written to " + "./generated/" + physicalGraph.label + ".png\n");
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();

            System.exit(0);
        }
        catch(IOException e)
        {
            e.printStackTrace();

            System.out.print("\nPlease check whether you have installed a dot renderer like GraphViz (http://www.graphviz.org/) and if the binaries are available via your PATH variable.\n");
            System.out.print("The generated dot file was not rendered to png.\n");
        }
    }
	
    public static void exportOperatorPlacementToDOTPNG(OperatorGraph operatorGraph, PhysicalGraph physicalGraph)
    {
        StringBuilder dot = new StringBuilder();

        dot.append("digraph PG\n");
        dot.append("{\n");
        dot.append("    graph [outputorder=edgesfirst, splines=true, dpi=200, fontname=\"Courier New Bold\"];\n\n");
        dot.append("    node [style=filled, fillcolor=white, color=black, fontname=\"Courier New Bold\"];\n\n");

        Node currentNode = null;
        
        Operator currentOperator = null;
        
        List<Operator> operators = null;
        
        for(int index = 0; index < physicalGraph.vertices.size(); index++)
        {
        	currentNode = (Node)physicalGraph.vertices.get(index);
        	
        	operators = operatorGraph.getOperatorsByAssignedID(currentNode.id);
        	
        	dot.append("    subgraph cluster_" + currentNode.id + "\n    {\n");
        	dot.append("        label=\"" + currentNode.label + "\";\n");
        	
        	for(int index2 = 0; index2 < operators.size(); index2++)
        	{
        		currentOperator = operators.get(index2);
        		
            	if(currentOperator.type.equals(Operator.Type.SOURCE))
            	{
            		dot.append("        " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=circle, width=1];\n");
            	}
            	else if(currentOperator.type.equals(Operator.Type.SINK))
            	{
            		dot.append("        " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=doublecircle, width=1];\n");
            	}
            	else if(currentOperator.type.equals(Operator.Type.MERGE))
            	{
            		dot.append("        " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=invtriangle, width=3];\n");
            	}
            	else if(currentOperator.type.equals(Operator.Type.PROJECT))
            	{
            		dot.append("        " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=tab, width=2];\n");
            	}
            	else if(currentOperator.type.equals(Operator.Type.PROCESSING))
            	{
            		dot.append("        " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=box, width=2];\n");
            	}
            	else if(currentOperator.type.equals(Operator.Type.BENCHMARK))
            	{
            		dot.append("        " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=note, width=2];\n");
            	}
            	else if(currentOperator.type.equals(Operator.Type.NOP))
            	{
            		dot.append("        " + currentOperator.id + " [group=g" + currentOperator.group + ", label=\"" + currentOperator.label + "\", shape=Msquare, width=2];\n");
            	}
            	else
            	{
            		System.err.println("No routine for Operator.Type." + currentOperator.type + " implemented.");
            		
            		System.exit(0);
            	}
        	}
        	
        	dot.append("    }\n\n");
        }

        dot.append("    edge [arrowhead=vee, arrowtail=none, color=black, fontname=\"Courier New Bold\", weight=1];\n");

        DataFlow currentDataFlow = null;
        
        for(int index = 0; index < operatorGraph.edges.size(); index++)
        {
        	currentDataFlow = (DataFlow)operatorGraph.edges.get(index);
        	
        	dot.append("    " + currentDataFlow.vertex0.id + ":s -> " + currentDataFlow.vertex1.id + ":n [label=\"" + currentDataFlow.label + "\"];\n");
        }

        dot.append("}");

        Util.writeFile("./generated/" + operatorGraph.label + ".dot", dot.toString(), Charset.defaultCharset());
        
        System.out.print("Written to " + "./generated/" + operatorGraph.label + ".dot\n");
        
        //dot -Tpng outputFilename.dot -o outputFilename.png
        ProcessBuilder builder = new ProcessBuilder("dot", "-Tpng", "./generated/" + operatorGraph.label + ".dot", "-o", "./generated/" + operatorGraph.label + ".png");
        builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);

        try
        {
            builder.start().waitFor();

            System.out.print("Written to " + "./generated/" + operatorGraph.label + ".png\n");
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();

            System.exit(0);
        }
        catch(IOException e)
        {
            e.printStackTrace();

            System.out.print("\nPlease check whether you have installed a dot renderer like GraphViz (http://www.graphviz.org/) and if the binaries are available via your PATH variable.\n");
            System.out.print("The generated dot file was not rendered to png.\n");
        }
    }
    
    public static void exportDockerComposeYAML(List<String> rpcServerSockets, List<String> sensors, PhysicalGraph physicalGraph)
    {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("version: \"3.9\"\r\n");
        stringBuilder.append("services:\r\n");

        String currentRPCServerSocket = null;
        String[] currentRPCServerSocketSplit = null;
        
		for(int index = 0; index < rpcServerSockets.size(); index++)
		{
			currentRPCServerSocket = rpcServerSockets.get(index);
			currentRPCServerSocketSplit = currentRPCServerSocket.split(":");
			
			stringBuilder.append("   " + ((currentRPCServerSocketSplit[0].equals("localhost"))?("rpc_classification_" + (index + 1)):currentRPCServerSocketSplit[0]) + ":\r\n");
			stringBuilder.append("      container_name: " + ((currentRPCServerSocketSplit[0].equals("localhost"))?("rpc_classification_" + (index + 1)):currentRPCServerSocketSplit[0]) + "\r\n");
			stringBuilder.append("      image: percom2022-python-rpc-classification\r\n");
			stringBuilder.append("      networks:\r\n");
			stringBuilder.append("         - percom2022-network\r\n");
			stringBuilder.append("      ports:\r\n");
			stringBuilder.append("         - \"" + (index + 9001) + ":" + currentRPCServerSocketSplit[1] + "\"\r\n");
			stringBuilder.append("      command: [\"" + currentRPCServerSocketSplit[1] + "\"]\r\n");
		}
		
		String currentSensor = null;
		
		int nodeCount = 0;
		
		for(int index = 0; index < sensors.size(); index++)
		{
			currentSensor = sensors.get(index);
			
			stringBuilder.append("   odysseus_" + currentSensor + ":\r\n");
			stringBuilder.append("      container_name: odysseus_" + currentSensor + "\r\n");
			stringBuilder.append("      image: percom2022-odysseus\r\n");
			stringBuilder.append("      networks:\r\n");
			stringBuilder.append("         - percom2022-network\r\n");
			stringBuilder.append("      ports:\r\n");
			stringBuilder.append("         - \"" + (9101 + nodeCount++) + ":8888\"\r\n");
		}

		Node currentNode = null;
		String[] currentNodeSocketSplit = null;
		
		for(int index = 0; index < physicalGraph.vertices.size(); index++)
		{
			currentNode = (Node)physicalGraph.vertices.get(index);
			
			if(!currentNode.type.equals(Type.EDGE))
			{
				currentNodeSocketSplit = currentNode.socket.split(":");
				
				stringBuilder.append("   " + currentNodeSocketSplit[0] + ":\r\n");
				stringBuilder.append("      container_name: " + currentNodeSocketSplit[0] + "\r\n");
				stringBuilder.append("      image: percom2022-odysseus\r\n");
				stringBuilder.append("      networks:\r\n");
				stringBuilder.append("         - percom2022-network\r\n");
				stringBuilder.append("      ports:\r\n");
				stringBuilder.append("         - \"" + (9101 + nodeCount++) + ":8888\"\r\n");
				stringBuilder.append("      expose:\r\n");
				//stringBuilder.append("         - \"" + currentNodeSocketSplit[1] + "\"\r\n");
				
				for(int index2 = 0; index2 < currentNode.ports.size(); index2++)
				{
					stringBuilder.append("         - \"" + currentNode.ports.get(index2) + "\"\r\n");
				}
			}
		}
		
		stringBuilder.append("networks:\r\n");
		stringBuilder.append("   percom2022-network:\r\n");
		stringBuilder.append("      external: true");
		
		Util.writeFile("./docker/docker-compose.yml", stringBuilder.toString(), Charset.defaultCharset());
		
		System.out.print("Written to ./docker/docker-compose.yml\n");
    }
    
    public static void exportGlobalQueryScript(List<String> partialPQLQueries)
    {
    	List<PartialQuery> partialQueries = new ArrayList<>();
    	
    	String currentPartialPQLQuery = null;
    	
    	for(int index = 0; index < partialPQLQueries.size(); index++)
    	{
    		currentPartialPQLQuery = partialPQLQueries.get(index);
    		
        	Server server = new Server();
        	server.setSocket("localhost:" + (9101 + index));
        	server.setUsername("System");
        	server.setPassword("manager");
        	
        	PartialQuery partialQuery = new PartialQuery();
        	partialQuery.setName("partialQuery_" + (index + 1));
        	partialQuery.setParser("OdysseusScript");
        	partialQuery.setQueryText(currentPartialPQLQuery);
        	partialQuery.setServer(server);
        	
        	partialQueries.add(partialQuery);
    	}
    	
    	GlobalQuery globalQuery = new GlobalQuery();
    	globalQuery.setName("globalQuery_1");
    	globalQuery.setPartialQueries(partialQueries);
		
		Util.writeFile("./generated/globalQuery_1.json", Util.toJson(globalQuery), Charset.defaultCharset());
		
		System.out.print("Written to ./generated/globalQuery_1.json\n");
    }
    
    /*
     * Source: https://stackoverflow.com/questions/1001290/console-based-progress-in-java
     * 
     * Eclipse Console Bug:
     * https://bugs.eclipse.org/bugs/show_bug.cgi?id=76936
     */
	public static void printProgressBar(int progress, int goal)
	{
		double progressPercentage = ((double)progress)/((double)goal);
		
	    final int width = 50; // progress bar width in chars

	    System.out.print("\r[");
	    
	    int i = 0;
	    
	    for(; i < (int)(progressPercentage*width); i++)
	    {
	    	System.out.print("=");
	    }
	    
	    for(; i < width; i++)
	    {
	    	System.out.print(" ");
	    }
	    
	    System.out.print("] ");
	    
	    String percentage = String.format("%.1f", progressPercentage * 100);
	    
	    if(percentage.length() == 3)
	    {
	    	percentage = "  " + percentage;
	    }
	    else if(percentage.length() == 4)
	    {
	    	percentage = " " + percentage;
	    }
	    
	    System.out.print(percentage + "% (" + progress + "/" + goal + ")");
	}
    
    /*
     * Source: http://www.java2s.com/example/java/reflection/find-the-closest-common-superclass-of-multiple-classes.html
     * 
     * Example Call:
     * FindCommonSuperclass(new Class[] {Double.class, Long.class, Integer.class, String.class})
     */
	@SuppressWarnings("rawtypes")
	public static Class FindCommonSuperclass(Class[] cs)
    {
        if (cs.length == 0) {
            return Object.class;
        } else if (cs.length == 1) {
            return cs[0];
        }

        // if any items fail getSuperclass in the passed in array,
        // simply return object.
        boolean isSame = true;
        boolean hasNullSuperclass = false;
        for (Class c : cs) {
            if (c == null)
                throw new NullPointerException();
            if (c != cs[0])
                isSame = false;
            if (c.getSuperclass() == null)
                hasNullSuperclass = true;
        }
        // no need to do further calculations.. all the same
        if (isSame)
            return cs[0];
        // at least one item in the list failed getSuperclass... return object
        if (hasNullSuperclass)
            return Object.class;

        Class c1 = cs[0];
        Class c2 = null;
        HashSet<Class> s1 = new HashSet<>();
        HashSet<Class> s2 = new HashSet<>();

        for (int i = 1; i < cs.length; i++) {
            s1.clear();
            s2.clear();
            c2 = cs[i];

            do {
                s1.add(c1);
                s2.add(c2);
                if (c1 != Object.class) {
                    c1 = c1.getSuperclass();
                }
                if (c2 != Object.class) {
                    c2 = c2.getSuperclass();
                }
            } while (Collections.disjoint(s1, s2));

            s1.retainAll(s2);
            c1 = s1.iterator().next(); // there can only be one
            if (c1 == Object.class)
                break; // no superclass above object
        }
        return c1;
    }
}