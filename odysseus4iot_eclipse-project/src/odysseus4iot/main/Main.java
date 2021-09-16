package odysseus4iot.main;

import java.util.List;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import odysseus4iot.graph.Graph;
import odysseus4iot.graph.operator.gen.OperatorGraphGenerator;
import odysseus4iot.graph.physical.gen.PhysicalGraphGenerator;
import odysseus4iot.model.Model;
import odysseus4iot.model.PostgresImport;
import odysseus4iot.util.Util;

//Timestamp from db problem
//Window problem with window slide
//activitypredict id problem with several sensors
//databasesink exception
/*
 * Operator Placement Query Optimization
 * 
 * using JGraphT (https://github.com/jgrapht/jgrapht)
 * 
 * Graph Interface implementation choice:
 * directed       = true
 * self-loops     = false
 * multiple edges = false
 * weighted       = true
 * => SimpleDirectedWeightedGraph/DefaultWeightedEdge
 * 
 * An operator graph is a DAG (directed acyclic graph)
 * 
 * Input:      Models/Nodes
 * Processing: 1. The logical operator graph for models is created
 *             2. The physical node graph is created
 *             3. All permutations of operator assignments to physical nodes are calculated
 *             4. The result is filtered regarding constraints and rated regarding a cost model
 *                4.1. All operators and data flow edges need to be assigned
 *                4.2. The order of operators need to be the same as before
 *                4.3. Cost model regarding network utilization and latency?
 *             5. Best placement is chosen
 *             6. Queries are created for that placement strategy
 * Output:     Queries for each single node
 */
public class Main
{
	public static Properties properties = null;
	
	public static void main(String[] args)
	{
		Util.charsetUTF8();
		
		//0 - Load property file
		InputStream input = null;
		
		try
		{
			input = new FileInputStream("./config.properties");
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
			System.exit(0);
		}

        properties = new Properties();

        try
        {
			properties.load(input);
		}
        catch(IOException e)
        {
			e.printStackTrace();
			System.exit(0);
		}
		
        Util.validateProperties();
        
		//1 - Input to System (sensors/nodes/labels)
		List<String> sensors = Arrays.asList(properties.getProperty("input.sensors").split(","));
		
		System.out.println("Input - Sensors");
		System.out.println("sensors = " + sensors + "\r\n");
		
		List<String> nodeNames = Arrays.asList(properties.getProperty("input.nodenames").split(","));
		List<String> nodeSockets = Arrays.asList(properties.getProperty("input.nodesockets").split(","));
		List<String> nodeTypes = Arrays.asList(properties.getProperty("input.nodetypes").split(","));
		List<String> nodeCPUCaps = Arrays.asList(properties.getProperty("input.nodecpucaps").split(","));
		List<String> nodeMemCaps = Arrays.asList(properties.getProperty("input.nodememcaps").split(","));
		List<String> edges = Arrays.asList(properties.getProperty("input.edges").split(","));
		List<String> edgeRateCaps = Arrays.asList(properties.getProperty("input.edgeratecaps").split(","));
		
		System.out.println("Input - Nodes");
		System.out.println("nodenames    = " + nodeNames);
		System.out.println("nodesockets  = " + nodeSockets);
		System.out.println("nodetypes    = " + nodeTypes);
		System.out.println("nodecpucaps  = " + nodeCPUCaps);
		System.out.println("nodememcaps  = " + nodeMemCaps);
		System.out.println("edges        = " + edges);
		System.out.println("edgeratecaps = " + edgeRateCaps + "\r\n");
		
		List<String> labels = Arrays.asList(properties.getProperty("input.labels").split(","));
		
		System.out.println("Input - Labels - (Not implemented)");
		System.out.println("labels = " + labels + "\r\n");
		
		//List<String> nodes = new ArrayList<>();
		
		//2 - Retrieving Model Information from Model Management System
		PostgresImport.url = "jdbc:postgresql://" + properties.getProperty("modeldb.host") + ":" + properties.getProperty("modeldb.port") + "/" + properties.getProperty("modeldb.database");
		PostgresImport.table = properties.getProperty("modeldb.table");
		PostgresImport.user = properties.getProperty("modeldb.user");
		PostgresImport.password = properties.getProperty("modeldb.password");

		List<Model> models = PostgresImport.importFromDB();
		
		//3 - Generating Logical Operator Graphs
		/*List<Graph> graphs = new ArrayList<>();
		
		Model currentModel = null;
		
		for(int index = 0; index < models.size(); index++)
		{
			currentModel = models.get(index);
			
			System.out.println(currentModel+"\r\n");
			
			Graph graph = OperatorGraphGenerator.generateOperatorGraph(sensors, currentModel);
			
			Util.exportPQL(currentModel.getModel_title(), graph);
			
			Util.exportDOTPNG(currentModel.getModel_title(), graph);
			
			graphs.add(graph);
		}*/
		
		boolean postprocessing = true;
		boolean merge = true;
		
		Graph operatorGraph = OperatorGraphGenerator.generateOperatorGraph(sensors, models, postprocessing, merge);
		
		Util.exportPQL("merged", operatorGraph);
		
		Util.exportOperatorGraphToDOTPNG("merged", operatorGraph);
		
		//4 - Generating Physical Graph
		
		Graph physicalGraph = PhysicalGraphGenerator.generatePhysicalraph(nodeNames, nodeSockets, nodeTypes, nodeCPUCaps, nodeMemCaps, edges, edgeRateCaps);
		
		Util.exportPhysicalGraphToDOTPNG("physical", physicalGraph);
	}
}