package odysseus4iot.main;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import odysseus4iot.graph.operator.gen.OperatorGraphGenerator;
import odysseus4iot.graph.operator.meta.OperatorGraph;
import odysseus4iot.graph.physical.gen.PhysicalGraphGenerator;
import odysseus4iot.graph.physical.meta.PhysicalGraph;
import odysseus4iot.model.Model;
import odysseus4iot.model.PostgresImport;
import odysseus4iot.model.management.ModelManagementRequest;
import odysseus4iot.model.management.ModelManagementRequestData;
import odysseus4iot.placement.OperatorPlacementOptimization;
import odysseus4iot.placement.OperatorPlacementPartitioner;
import odysseus4iot.placement.model.OperatorPlacement;
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
	
	public static Double evaluationSpeedupFactor = 2.0d;

	public static void main(String[] args)
	{
		Util.charsetUTF8();
		
		//1 - Input to System (sensors/nodes/labels)
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
		
		List<Integer> ids = Arrays.asList(properties.getProperty("input.ids").split(",")).stream().map(Integer::parseInt).collect(Collectors.toList());
		List<String> rpcServerSockets = Arrays.asList(properties.getProperty("pythonrpc.sockets").split(","));
		
		ModelManagementRequestData modelManagementRequestData = new ModelManagementRequestData();
		modelManagementRequestData.sensor_system = "Blaupunkt_BST-BNO055-DS000-14_NDOF_10_AO";
		
		List<String> labels = Arrays.asList(properties.getProperty("input.labels").split(","));
		List<Double> accs = Arrays.asList(properties.getProperty("input.accs").split(",")).stream().map(Double::parseDouble).collect(Collectors.toList());
		
		for(int index = 0; index < labels.size(); index++)
		{
			modelManagementRequestData.addLabel(labels.get(index), accs.get(index));
		}
		
		modelManagementRequestData.maxsizeofbignode = 28000;
		modelManagementRequestData.maxsizeof_ALL = 28000;
		modelManagementRequestData.modelset_limit = 3;
		
		ModelManagementRequest modelManagementRequest = new ModelManagementRequest();
		modelManagementRequest.request = modelManagementRequestData;
		
		System.out.println("Generated ModelManagementRequest:");
		System.out.println(Util.toJson(modelManagementRequest)+"\n");
		
		//2 - Retrieving Model Information from Model Management System
		PostgresImport.url = "jdbc:postgresql://" + properties.getProperty("modeldb.host") + ":" + properties.getProperty("modeldb.port") + "/" + properties.getProperty("modeldb.database");
		PostgresImport.table = properties.getProperty("modeldb.table");
		PostgresImport.user = properties.getProperty("modeldb.user");
		PostgresImport.password = properties.getProperty("modeldb.password");

		List<List<Model>> modelsets = new ArrayList<>();
		
		List<List<Integer>> modelsetIDs = new ArrayList<>();
		
		boolean hardcoded = true;
		
		if(hardcoded)
		{
			List<Integer> modelsetIDs0 = new ArrayList<>();
			modelsetIDs0.add(4271);
			modelsetIDs0.add(4272);
			
			List<Integer> modelsetIDs1 = new ArrayList<>();
			modelsetIDs1.add(4273);
			modelsetIDs1.add(4274);
			
			modelsetIDs.add(modelsetIDs0);
			modelsetIDs.add(modelsetIDs1);
		}
		else
		{
			modelsetIDs.add(ids);
		}
		
		List<Model> models = null;
		
		for(int index = 0; index < modelsetIDs.size(); index++)
		{
			System.out.println("Loading models=" + modelsetIDs.get(index));
			
			models = PostgresImport.importFromTrainedModelsSchema(modelsetIDs.get(index));
			
			if(models.size() != modelsetIDs.get(index).size())
			{
				System.err.println("Model ids not found in the database!");
				
				System.exit(0);
			}
			
			for(int index2 = 0; index2 < models.size(); index2++)
			{
				models.get(index2).setRpcServerSocket(rpcServerSockets.get(index2));
			}
			
			modelsets.add(models);
		}
		
		System.out.println("");
		
		//3 - Generating Logical Operator Graph for each model
		/*List<OperatorGraph> graphs = new ArrayList<>();
		
		Model currentModel = null;
		
		boolean postprocessing = true;
		boolean merge = true;
		
		for(int index = 0; index < models.size(); index++)
		{
			currentModel = models.get(index);
			
			currentModel.setRpcServerSocket(rpcServerSockets.get(index));
			
			System.out.println(currentModel.printDetails()+"\r\n");
			
			List<Model> singleModel = new ArrayList<>();
			singleModel.add(currentModel);
			
			operatorGraph = OperatorGraphGenerator.generateOperatorGraph(sensors, singleModel, postprocessing, merge);
			
			Util.exportPQL(operatorGraph);
			
			Util.exportOperatorGraphToDOTPNG(operatorGraph);
			
			System.out.print("\r\n");
			
			graphs.add(operatorGraph);
		}*/
		
		//4 - Generating Physical Graph
		PhysicalGraph physicalGraph = PhysicalGraphGenerator.generatePhysicalGraph(nodeNames, nodeSockets, nodeTypes, nodeCPUCaps, nodeMemCaps, edges, edgeRateCaps);
		
		Util.exportPhysicalGraphToDOTPNG(physicalGraph);
		
		System.out.print("\r\n");
		
		List<OperatorPlacement> operatorPlacementsGlobal = new ArrayList<>();
		
		for(int index = 0; index < modelsets.size(); index++)
		{
			models = modelsets.get(index);
			
			//5 - Generating Merged Logical Operator Graph for all models
			OperatorGraph operatorGraph = OperatorGraphGenerator.generateOperatorGraph(sensors, models, true, true);
			
			//Util.exportPQL(operatorGraph);
			
			//Util.exportOperatorGraphToDOTPNG(operatorGraph);
			
			//System.out.print("\r\n");
			
			//6 - Perform Operator Placement Optimization for merged operator graph and physical graph
			List<OperatorPlacement> operatorPlacements = OperatorPlacementOptimization.optimize(operatorGraph, physicalGraph);
			
			operatorPlacementsGlobal.addAll(operatorPlacements);
		}
		
		if(operatorPlacementsGlobal.isEmpty())
		{
			System.err.println("No valid operator placements found!");
			
			System.exit(0);
		}
		
		System.out.println(operatorPlacementsGlobal.size() + " operator placements verified for " + modelsets.size() + " modelsets\n");
		
		Collections.sort(operatorPlacementsGlobal);
		
		for(int index = 0; index < operatorPlacementsGlobal.size(); index++)
		{
			System.out.println(operatorPlacementsGlobal.get(index));
		}
		
		OperatorGraph operatorGraph = operatorPlacementsGlobal.get(0).operatorGraph;
		
		boolean successfulLoading = operatorGraph.loadOperatorPlacement(operatorPlacementsGlobal.get(0), physicalGraph);
		
		if(successfulLoading)
		{
			System.out.println("Successfully loaded placement strategy " + operatorPlacementsGlobal.get(0) + "\n");
		}
		else
		{
			System.err.println("Error loading placement strategy " + operatorPlacementsGlobal.get(0));
			
			System.exit(0);
		}
		
		Util.exportOperatorPlacementToDOTPNG(operatorGraph, physicalGraph);
		
		System.out.print("\r\n");
		
		//7 - Transformation to distributed operator graph
		//OperatorPlacementPartitioner.transformOperatorGraphToDistributed(operatorGraph, physicalGraph);
		//OperatorPlacementPartitioner.addBenchmarkOperators(operatorGraph, physicalGraph);
		OperatorPlacementPartitioner.addBenchmarkOperatorsSingleNode(operatorGraph, physicalGraph);
		
		Util.exportPQL(operatorGraph);
		Util.exportOperatorPlacementToDOTPNG(operatorGraph, physicalGraph);
		
		System.out.print("\r\n");
		
		//8 - Generation of subgraphs for distribution
		/*List<OperatorGraph> subGraphs = OperatorPlacementPartitioner.buildSubgraphs(operatorGraph, physicalGraph);
		
		for(int index = 0; index < subGraphs.size(); index++)
		{
			Util.exportPQL(subGraphs.get(index));
			Util.exportOperatorGraphToDOTPNG(subGraphs.get(index));
		}*/
	}
}