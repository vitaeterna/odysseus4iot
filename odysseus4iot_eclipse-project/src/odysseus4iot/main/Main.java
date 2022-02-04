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

//TODO: _ Window problem with window slide
//TODO: _ Improve .dot generation
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
/**
 * This is the main entrypoint of the Odysseus4IoT query optimization module.
 * 
 * @author Michael Sünkel
 */
public class Main
{
	public static Properties properties = null;
	
	//Parameters
	public static Integer evalCase = 1;
	public static String configProperties = null;
	
	public static Double evaluationSpeedupFactor = 1.0d;
	public static boolean postprocessing = false;
	
	public static boolean dotpng = false;
	public static boolean distributed = true;
	public static boolean benchmark = true;

	/**
	 * This is the main entrypoint of the Odysseus4IoT query optimization module.
	 * 
	 * @param args
	 */
	public static void main(String[] args)
	{
		Long startTimestamp = System.currentTimeMillis();
		Long endTimestamp = null;
		
		Util.charsetUTF8();
		
		if(args.length > 0)
		{
			evalCase = Integer.parseInt(args[0]);
		}
		else
		{
			System.out.println("Please provide the number of an evaluation case as command line parameter. [1-5]");
			System.exit(0);
		}
		
		switch(evalCase.intValue())
		{
			case 0:
				distributed = false;
			case 1:
				configProperties = "./config_ec_docker.properties";
				break;
			case 2:
			case 3:
			case 4:
			case 5:
				configProperties = "./config_efc_docker.properties";
				break;
			default:
				configProperties = "./config_efc_docker.properties";
				break;
		}
		
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
		List<String> edgeDelays = Arrays.asList(properties.getProperty("input.edgedelays").split(","));
		
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
		//PostgresImport.url = "jdbc:postgresql://" + properties.getProperty("modeldb.host") + ":" + properties.getProperty("modeldb.port") + "/" + properties.getProperty("modeldb.database");
		PostgresImport.url = "jdbc:postgresql://localhost:" + properties.getProperty("modeldb.port") + "/" + properties.getProperty("modeldb.database");
		PostgresImport.table = properties.getProperty("modeldb.table");
		PostgresImport.user = properties.getProperty("modeldb.user");
		PostgresImport.password = properties.getProperty("modeldb.password");

		List<List<Model>> modelsets = new ArrayList<>();
		
		List<List<Integer>> modelsetIDs = new ArrayList<>();
		
		switch(evalCase.intValue())
		{
			case 0: //Test case
				List<Integer> modelsetIDsCase0_1 = new ArrayList<>();
				modelsetIDsCase0_1.add(44086);
				modelsetIDsCase0_1.add(44088);
				modelsetIDsCase0_1.add(44089);
				
				modelsetIDs.add(modelsetIDsCase0_1);
				break;
			case 1: //Manually selected models deployed on            cloud (no operator placement) - using config_ec_docker.properties
			case 2: //Manually selected models deployed on edge, fog, cloud (   operator placement) - using config_efc_docker.properties
				List<Integer> modelsetIDsCase12_1 = new ArrayList<>();
				modelsetIDsCase12_1.add(44086);
				modelsetIDsCase12_1.add(44088);
				modelsetIDsCase12_1.add(44089);
				
				modelsetIDs.add(modelsetIDsCase12_1);
				break;
			case 3: //MLMM 1.0 selected models deployed on edge, fog, cloud (   operator placement) - using config_efc_docker.properties
				List<Integer> modelsetIDsCase13_1 = new ArrayList<>();
				modelsetIDsCase13_1.add(9925);
				modelsetIDsCase13_1.add(15875);
				modelsetIDsCase13_1.add(24179);
				
				List<Integer> modelsetIDsCase13_2 = new ArrayList<>();
				modelsetIDsCase13_2.add(15875);
				modelsetIDsCase13_2.add(24179);
				modelsetIDsCase13_2.add(44089);
				
				List<Integer> modelsetIDsCase13_3 = new ArrayList<>();
				modelsetIDsCase13_3.add(9925);
				modelsetIDsCase13_3.add(15945);
				modelsetIDsCase13_3.add(24179);
				
				modelsetIDs.add(modelsetIDsCase13_1);
				modelsetIDs.add(modelsetIDsCase13_2);
				modelsetIDs.add(modelsetIDsCase13_3);
				break;
			case 4: //MLMM 0.7 selected models deployed on edge, fog, cloud (   operator placement) - using config_efc_docker.properties
				List<Integer> modelsetIDsCase14_1 = new ArrayList<>();
				modelsetIDsCase14_1.add(9925);
				modelsetIDsCase14_1.add(15640);
				modelsetIDsCase14_1.add(23764);
				
				List<Integer> modelsetIDsCase14_2 = new ArrayList<>();
				modelsetIDsCase14_2.add(9925);
				modelsetIDsCase14_2.add(15640);
				modelsetIDsCase14_2.add(23820);
				
				List<Integer> modelsetIDsCase14_3 = new ArrayList<>();
				modelsetIDsCase14_3.add(9925);
				modelsetIDsCase14_3.add(15720);
				modelsetIDsCase14_3.add(23764);
				
				modelsetIDs.add(modelsetIDsCase14_1);
				modelsetIDs.add(modelsetIDsCase14_2);
				modelsetIDs.add(modelsetIDsCase14_3);
				break;
			case 5: //MLMM 0.5 selected models deployed on edge, fog, cloud (   operator placement) - using config_efc_docker.properties
				List<Integer> modelsetIDsCase15_1 = new ArrayList<>();
				modelsetIDsCase15_1.add(5940);
				modelsetIDsCase15_1.add(15305);
				
				List<Integer> modelsetIDsCase15_2 = new ArrayList<>();
				modelsetIDsCase15_2.add(6211);
				modelsetIDsCase15_2.add(15860);
				
				List<Integer> modelsetIDsCase15_3 = new ArrayList<>();
				modelsetIDsCase15_3.add(5896);
				modelsetIDsCase15_3.add(15225);
				
				modelsetIDs.add(modelsetIDsCase15_1);
				modelsetIDs.add(modelsetIDsCase15_2);
				modelsetIDs.add(modelsetIDsCase15_3);
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
		PhysicalGraph physicalGraph = PhysicalGraphGenerator.generatePhysicalGraph(nodeNames, nodeSockets, nodeTypes, nodeCPUCaps, nodeMemCaps, edges, edgeRateCaps, edgeDelays);
		
		if(dotpng)
		{
			Util.exportPhysicalGraphToDOTPNG(physicalGraph);
			
			System.out.print("\r\n");
		}
		
		List<OperatorPlacement> operatorPlacementsGlobal = new ArrayList<>();
		
		List<OperatorGraph> operatorGraphs = new ArrayList<>();
		
		OperatorGraph operatorGraph = null;
		
		List<Long> optimizationTimes = new ArrayList<>();
		
		for(int index = 0; index < modelsets.size(); index++)
		{
			models = modelsets.get(index);
			
			//5 - Generating Merged Logical Operator Graph for all models
			operatorGraph = OperatorGraphGenerator.generateOperatorGraph(sensors, models, postprocessing);
			
			operatorGraphs.add(operatorGraph);
			
			Util.exportPQL(operatorGraph);
			
			if(dotpng)
			{
				Util.exportOperatorGraphToDOTPNG(operatorGraph);
				
				System.out.print("\r\n");
			}
			
			if(distributed)
			{
				//6 - Perform Operator Placement Optimization for merged operator graph and physical graph
				List<OperatorPlacement> operatorPlacements = OperatorPlacementOptimization.optimize(operatorGraph, physicalGraph, optimizationTimes);
				
				operatorPlacementsGlobal.addAll(operatorPlacements);
			}
		}
		
		//7 - Transformation to distributed operator graph
		if(distributed)
		{
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
			
			operatorGraph = operatorPlacementsGlobal.get(0).operatorGraph;
			operatorGraphs.clear();
			operatorGraphs.add(operatorGraph);
			
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
			
			OperatorPlacementPartitioning.transformOperatorGraphToDistributed(operatorGraph, physicalGraph);
		}
		
		for(int index = 0; index < operatorGraphs.size(); index++)
		{
			operatorGraph = operatorGraphs.get(index);
			
			if(benchmark)
			{
				OperatorPlacementBenchmark.addBenchmarkOperators(operatorGraph, physicalGraph);
			}
			
			Util.exportPQL(operatorGraph);
			
			if(dotpng)
			{
				if(distributed)
				{
					Util.exportOperatorPlacementToDOTPNG(operatorGraph, physicalGraph);
					
					System.out.print("\r\n");
				}
				else
				{
					if(benchmark)
					{
						Util.exportOperatorGraphToDOTPNG(operatorGraph);
						
						System.out.print("\r\n");
					}
				}
			}
		}
		
		if(distributed)
		{
			//8 - Generation of subgraphs for distribution
			List<OperatorGraph> subGraphs = OperatorPlacementPartitioning.buildSubgraphs(operatorGraph, physicalGraph);
			
			if(dotpng)
			{
				for(int index = 0; index < subGraphs.size(); index++)
				{
					Util.exportOperatorGraphToDOTPNG(subGraphs.get(index));
				}
			}
			
			//9 - Generation of Docker Compose YAML
			Util.exportDockerComposeYAML(rpcServerSockets, sensors, physicalGraph);
			
			//10 - Generation of Global Query Script (Deployment File)
			Util.exportGlobalQueryScript(subGraphs);
			
			System.out.print("\r\n");
		}
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println("Operator Placement Optimization took " + Util.formatTimestamp(optimizationTimes.stream().mapToLong(Long::longValue).sum()) + "\r\n");
		
		System.out.println("Finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + " OperatorCounts: " + operatorGraph.getNumberOfOperatorsPerPipelineStep());
	}
}