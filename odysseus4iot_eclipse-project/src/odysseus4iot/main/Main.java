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
import odysseus4iot.placement.OperatorPlacementBenchmark;
import odysseus4iot.placement.OperatorPlacementOptimization;
import odysseus4iot.placement.OperatorPlacementPartitioning;
import odysseus4iot.placement.model.OperatorPlacement;
import odysseus4iot.util.Util;

//TODO: __ Window problem with window slide
/*
 * Operator Placement Query Optimization
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
	
	//Parameters
	public static Integer evalCase = 1;
	
	public static Double evaluationSpeedupFactor = 1.0d;
	public static boolean postprocessing = false;
	public static boolean merge = true;
	
	public static boolean dotpng = true;
	public static boolean distributed = false;
	
	public static String configProperties = "./config_ec.properties";

	public static void main(String[] args)
	{
		Long startTimestamp = System.currentTimeMillis();
		Long endTimestamp = null;
		
		Util.charsetUTF8();
		
		//1 - Input to System (sensors/nodes/labels)
		InputStream input = null;
		
		try
		{
			input = new FileInputStream(configProperties);
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
		
		/*ModelManagementRequestData modelManagementRequestData = new ModelManagementRequestData();
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
		System.out.println(Util.toJson(modelManagementRequest)+"\n");*/
		
		//2 - Retrieving Model Information from Model Management System
		PostgresImport.url = "jdbc:postgresql://" + properties.getProperty("modeldb.host") + ":" + properties.getProperty("modeldb.port") + "/" + properties.getProperty("modeldb.database");
		PostgresImport.table = properties.getProperty("modeldb.table");
		PostgresImport.user = properties.getProperty("modeldb.user");
		PostgresImport.password = properties.getProperty("modeldb.password");

		List<List<Model>> modelsets = new ArrayList<>();
		
		List<List<Integer>> modelsetIDs = new ArrayList<>();
		
		switch(evalCase.intValue())
		{
			case 1:
				//Case 1 - Sensors, Cloud - 1.0 acc 18730_27039_11340 (problem: all models are window aligned)
				List<Integer> modelsetIDsCase12_1 = new ArrayList<>();
				modelsetIDsCase12_1.add(11340);
				modelsetIDsCase12_1.add(18730);
				modelsetIDsCase12_1.add(27039);
				
				modelsetIDs.add(modelsetIDsCase12_1);
			case 2:
				//Case 2 - Sensors, Fog, Cloud - 1.0 acc 18730_27039_11340 (problem: all models are window aligned)
				break;
			case 3:
				//Case 3 - Sensors, Fog, Cloud - 0.5 acc edge: 16499_6529, 18444_7568, 17809_7253 fog: 18400_7535, 18400_7539, 17765_7224
				List<Integer> modelsetIDsCase13_1 = new ArrayList<>();
				modelsetIDsCase13_1.add(6529);
				modelsetIDsCase13_1.add(16499);
				
				List<Integer> modelsetIDsCase13_2 = new ArrayList<>();
				modelsetIDsCase13_2.add(7568);
				modelsetIDsCase13_2.add(18444);
				
				List<Integer> modelsetIDsCase13_3 = new ArrayList<>();
				modelsetIDsCase13_3.add(7253);
				modelsetIDsCase13_3.add(17809);
				
				List<Integer> modelsetIDsCase13_4 = new ArrayList<>();
				modelsetIDsCase13_4.add(7535);
				modelsetIDsCase13_4.add(18400);
				
				List<Integer> modelsetIDsCase13_5 = new ArrayList<>();
				modelsetIDsCase13_5.add(7539);
				modelsetIDsCase13_5.add(18400);
				
				List<Integer> modelsetIDsCase13_6 = new ArrayList<>();
				modelsetIDsCase13_6.add(7224);
				modelsetIDsCase13_6.add(17765);
				
				modelsetIDs.add(modelsetIDsCase13_1);
				modelsetIDs.add(modelsetIDsCase13_2);
				modelsetIDs.add(modelsetIDsCase13_3);
				modelsetIDs.add(modelsetIDsCase13_4);
				modelsetIDs.add(modelsetIDsCase13_5);
				modelsetIDs.add(modelsetIDsCase13_6);
				break;
			case 4:
				//Case 4 - Sensors, Fog, Cloud - 0.0 acc edge: 11979_4309, 11979_20333_4309, 11979_20333_8158 fog: 11916_4272, 11916_4273, 11916_4275
				List<Integer> modelsetIDsCase14_1 = new ArrayList<>();
				modelsetIDsCase14_1.add(6529);
				modelsetIDsCase14_1.add(16499);
				
				List<Integer> modelsetIDsCase14_2 = new ArrayList<>();
				modelsetIDsCase14_2.add(7568);
				modelsetIDsCase14_2.add(18444);
				
				List<Integer> modelsetIDsCase14_3 = new ArrayList<>();
				modelsetIDsCase14_3.add(7253);
				modelsetIDsCase14_3.add(17809);
				
				List<Integer> modelsetIDsCase14_4 = new ArrayList<>();
				modelsetIDsCase14_4.add(7535);
				modelsetIDsCase14_4.add(18400);
				
				List<Integer> modelsetIDsCase14_5 = new ArrayList<>();
				modelsetIDsCase14_5.add(7539);
				modelsetIDsCase14_5.add(18400);
				
				List<Integer> modelsetIDsCase14_6 = new ArrayList<>();
				modelsetIDsCase14_6.add(7224);
				modelsetIDsCase14_6.add(17765);
				
				modelsetIDs.add(modelsetIDsCase14_1);
				modelsetIDs.add(modelsetIDsCase14_2);
				modelsetIDs.add(modelsetIDsCase14_3);
				modelsetIDs.add(modelsetIDsCase14_4);
				modelsetIDs.add(modelsetIDsCase14_5);
				modelsetIDs.add(modelsetIDsCase14_6);
				break;
			default:
				modelsetIDs.add(ids);
				break;
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
			
			if(dotpng)
			{
				Util.exportOperatorGraphToDOTPNG(operatorGraph);
			}
			
			System.out.print("\r\n");
			
			graphs.add(operatorGraph);
		}*/
		
		//4 - Generating Physical Graph
		PhysicalGraph physicalGraph = PhysicalGraphGenerator.generatePhysicalGraph(nodeNames, nodeSockets, nodeTypes, nodeCPUCaps, nodeMemCaps, edges, edgeRateCaps);
		
		if(dotpng)
		{
			Util.exportPhysicalGraphToDOTPNG(physicalGraph);
		}
		
		System.out.print("\r\n");
		
		List<OperatorPlacement> operatorPlacementsGlobal = new ArrayList<>();
		
		for(int index = 0; index < modelsets.size(); index++)
		{
			models = modelsets.get(index);
			
			//5 - Generating Merged Logical Operator Graph for all models
			OperatorGraph operatorGraph = OperatorGraphGenerator.generateOperatorGraph(sensors, models, postprocessing, merge);
			
			/*Util.exportPQL(operatorGraph);
			
			if(dotpng)
			{
				Util.exportOperatorGraphToDOTPNG(operatorGraph);
			}
			
			System.out.print("\r\n");*/
			
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
			System.out.println("Successfully loaded placement strategy " + operatorPlacementsGlobal.get(0));
		}
		else
		{
			System.err.println("Error loading placement strategy " + operatorPlacementsGlobal.get(0));
			
			System.exit(0);
		}
		
		if(dotpng)
		{
			Util.exportOperatorPlacementToDOTPNG(operatorGraph, physicalGraph);
		}
		
		System.out.print("\r\n");
		
		//7 - Transformation to distributed operator graph
		if(distributed)
		{
			OperatorPlacementPartitioning.transformOperatorGraphToDistributed(operatorGraph, physicalGraph);
		}
		
		OperatorPlacementBenchmark.addBenchmarkOperators(operatorGraph, physicalGraph);
		
		String globalQuery = Util.exportPQL(operatorGraph);
		
		if(dotpng)
		{
			Util.exportOperatorPlacementToDOTPNG(operatorGraph, physicalGraph);
		}
		
		System.out.print("\r\n");
		
		//8 - Generation of subgraphs for distribution
		List<String> partialPQLQueries = new ArrayList<>();
		
		if(distributed)
		{
			List<OperatorGraph> subGraphs = OperatorPlacementPartitioning.buildSubgraphs(operatorGraph, physicalGraph);
			
			for(int index = 0; index < subGraphs.size(); index++)
			{
				partialPQLQueries.add(Util.exportPQL(subGraphs.get(index)));
				
				if(dotpng)
				{
					Util.exportOperatorGraphToDOTPNG(subGraphs.get(index));
				}
			}
		}
		else
		{
			partialPQLQueries.add(globalQuery);
		}
		
		//9 - Generation of Docker Compose YAMNL
		Util.exportDockerComposeYAML(rpcServerSockets, sensors, nodeSockets, nodeTypes);
		
		//10 - Generation of Global Query Script (Deployment File)
		Util.exportGlobalQueryScript(partialPQLQueries);
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println("\r\nFinished after " + Util.formatTimestamp(endTimestamp - startTimestamp));
	}
}