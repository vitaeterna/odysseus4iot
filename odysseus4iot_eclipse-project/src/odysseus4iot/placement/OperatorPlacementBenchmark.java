package odysseus4iot.placement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import odysseus4iot.graph.operator.CalcLatencyOperator;
import odysseus4iot.graph.operator.DatabasesinkOperator;
import odysseus4iot.graph.operator.DatarateOperator;
import odysseus4iot.graph.operator.MapOperator;
import odysseus4iot.graph.operator.SenderOperator;
import odysseus4iot.graph.operator.SleepOperator;
import odysseus4iot.graph.operator.gen.OperatorGenerator;
import odysseus4iot.graph.operator.meta.Column;
import odysseus4iot.graph.operator.meta.DataFlow;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.graph.operator.meta.OperatorGraph;
import odysseus4iot.graph.operator.meta.Operator.Type;
import odysseus4iot.graph.physical.meta.Connection;
import odysseus4iot.graph.physical.meta.Node;
import odysseus4iot.graph.physical.meta.PhysicalGraph;
import odysseus4iot.util.Util;

/**
 * The {@code OperatorPlacementBenchmark} provides methods to enrich an operator graph by measurement operators like e.g. CalcLatency or Datarate.
 * 
 * @author Michael Sünkel
 */
public class OperatorPlacementBenchmark
{
	private static Map<String, Integer> perNodeCount = new HashMap<>();
	
	public static void addBenchmarkOperators(OperatorGraph operatorGraph, PhysicalGraph physicalGraph)
	{
		perNodeCount.clear();
		
		Long startTimestamp = System.currentTimeMillis();
		Long endTimestamp = null;
		
		System.out.println("Adding benchmark operators to Operator Graph " + operatorGraph.label + "...");
		
		operatorGraph.label += "_benchmark";
		
		DataFlow currentDataFlow = null;
		
		Operator operator0 = null;
		Operator operator1 = null;
		
		Node currentNode = null;
		
		List<DataFlow> edgesToAdd = new ArrayList<>();
		List<DataFlow> edgesToRemove = new ArrayList<>();
		
		//Add benchmark operators directly before the sink
		for(int index = 0; index < operatorGraph.edges.size(); index++)
		{
			currentDataFlow = (DataFlow)operatorGraph.edges.get(index);
			
			operator0 = (Operator)currentDataFlow.vertex0;
			operator1 = (Operator)currentDataFlow.vertex1;
			
			currentNode = physicalGraph.getNodeByID(operator0.assignedID);
			
			if(currentNode == null)
			{
				currentNode = new Node("benchmark", null, null, null, null);
			}
			
			if(operator0.assignedID.intValue() == operator1.assignedID.intValue())
			{
				if(operator1 instanceof DatabasesinkOperator)
				{
					edgesToRemove.add(currentDataFlow);
					
					//CalcLatencyOperator
					CalcLatencyOperator calclatencyOperator = new CalcLatencyOperator();
					
					calclatencyOperator.type = Type.BENCHMARK;
					
					calclatencyOperator.assignedID = operator0.assignedID;
					
					calclatencyOperator.inputSchema = operator0.outputSchema.copy();
					calclatencyOperator.inputRate = operator0.outputRate;
					calclatencyOperator.inputName = operator0.outputName;

					calclatencyOperator.outputSchema = calclatencyOperator.inputSchema.copy();
					calclatencyOperator.outputRate = calclatencyOperator.inputRate;
					calclatencyOperator.outputName = "latency_" + currentNode.name;
					
					operatorGraph.addVertex(calclatencyOperator, false);
					edgesToAdd.add(new DataFlow(operator0, calclatencyOperator));
					
					//Datarate Operator
					DatarateOperator datarateOperator = OperatorGenerator.generateDatarateOperator("datarate_" + currentNode.name);
					
					datarateOperator.type = Type.BENCHMARK;
					
					datarateOperator.assignedID = operator0.assignedID;
					
					datarateOperator.inputSchema = calclatencyOperator.outputSchema.copy();
					datarateOperator.inputRate = calclatencyOperator.outputRate;
					datarateOperator.inputName = calclatencyOperator.outputName;

					datarateOperator.outputSchema = datarateOperator.inputSchema.copy();
					datarateOperator.outputRate = datarateOperator.inputRate;

					operatorGraph.addVertex(datarateOperator, false);
					edgesToAdd.add(new DataFlow(calclatencyOperator, datarateOperator));
					
					//MapOperator
					MapOperator mapOperator = new MapOperator();
					
					mapOperator.type = Type.BENCHMARK;
					
					mapOperator.assignedID = operator0.assignedID;
					
					mapOperator.inputSchema = datarateOperator.outputSchema.copy();
					mapOperator.inputRate = datarateOperator.outputRate;
					mapOperator.inputName = datarateOperator.outputName;
					
					List<String> epressions = mapOperator.inputSchema.toStringList();
					
					mapOperator.expressions = new ArrayList<>();
					
					for(int index2 = 0; index2 < epressions.size(); index2++)
					{
						mapOperator.expressions.add("'" + epressions.get(index2) + "'");
					}
					
					mapOperator.expressions.add("['TimeInterval.start', 'time_start']");
					mapOperator.expressions.add("['TimeInterval.end', 'time_end']");
					mapOperator.expressions.add("['ToInteger(elementAt(Datarate.Measurements[0],1))', 'bytes_sent']");
					mapOperator.expressions.add("['ToLong((Latency.lend-Latency.minlstart)/1000000)','minLatencyInMS']");
					mapOperator.expressions.add("['ToLong((Latency.lend-Latency.maxlstart)/1000000)','maxLatencyInMS']");
					
					mapOperator.outputSchema = mapOperator.inputSchema.copy();
					mapOperator.outputSchema.addColumn(new Column("time_start", Long.class));
					mapOperator.outputSchema.addColumn(new Column("time_end", Long.class));
					mapOperator.outputSchema.addColumn(new Column("bytes_sent", Integer.class));
					mapOperator.outputSchema.addColumn(new Column("minLatencyInMS", Long.class));
					mapOperator.outputSchema.addColumn(new Column("maxLatencyInMS", Long.class));
					mapOperator.outputRate = mapOperator.inputRate;
					mapOperator.outputName = "metadata_map_" + currentNode.name;
					
					operatorGraph.addVertex(mapOperator, false);
					edgesToAdd.add(new DataFlow(datarateOperator, mapOperator));
					
					//DatabasesinkOperator
					operator1.inputSchema = mapOperator.outputSchema.copy();
					operator1.inputRate = mapOperator.outputRate;
					operator1.inputName = mapOperator.outputName;
					
					operator1.outputSchema = operator1.inputSchema.copy();
					operator1.outputRate = operator1.inputRate;
					
					edgesToAdd.add(new DataFlow(mapOperator, operator1));
				}
			}
		}
		
		operatorGraph.edges.removeAll(edgesToRemove);
		operatorGraph.edges.addAll(edgesToAdd);
		
		edgesToRemove.clear();
		edgesToAdd.clear();
		
		//Add benchmark operators next to each Sender-Operator
		for(int index = 0; index < operatorGraph.edges.size(); index++)
		{
			currentDataFlow = (DataFlow)operatorGraph.edges.get(index);
			
			operator0 = (Operator)currentDataFlow.vertex0;
			operator1 = (Operator)currentDataFlow.vertex1;
			
			currentNode = physicalGraph.getNodeByID(operator0.assignedID);
			
			if(currentNode == null)
			{
				currentNode = new Node("benchmark", null, null, null, null);
			}
			
			if(operator0.assignedID.intValue() != operator1.assignedID.intValue())
			{
				int currentCount = getPerNodeCount(currentNode.name);
				
				if(operator0 instanceof SenderOperator)
				{
					int edgeDelay = ((Connection)physicalGraph.getEdge(operator0.assignedID.intValue(), operator1.assignedID.intValue())).delay;
					
					operator1 = operator0;
					operator0 = (Operator)operatorGraph.getPredecessors(operator0).get(0);
					
					currentDataFlow = (DataFlow)operatorGraph.getEdge(operator0, operator1);
					
					//Add sleep operator to simulate network delay
					if(edgeDelay > 0)
					{
						edgesToRemove.add(currentDataFlow);
						
						SleepOperator sleepOperator = new SleepOperator(edgeDelay);
						
						sleepOperator.type = Type.BENCHMARK;
						
						sleepOperator.assignedID = operator0.assignedID;
						
						sleepOperator.inputSchema = operator0.outputSchema.copy();
						sleepOperator.inputRate = operator0.outputRate;
						sleepOperator.inputName = operator0.outputName;

						sleepOperator.outputSchema = sleepOperator.inputSchema.copy();
						sleepOperator.outputRate = sleepOperator.inputRate;
						sleepOperator.outputName = "sleep_" + currentNode.name + "_" + currentCount;
						
						operator1.inputName = sleepOperator.outputName;
						
						operatorGraph.addVertex(sleepOperator, false);
						edgesToAdd.add(new DataFlow(operator0, sleepOperator));
						edgesToAdd.add(new DataFlow(sleepOperator, operator1));
					}
				}
				
				//CalcLatencyOperator
				CalcLatencyOperator calclatencyOperator = new CalcLatencyOperator();
				
				calclatencyOperator.type = Type.BENCHMARK;
				
				calclatencyOperator.assignedID = operator0.assignedID;
				
				calclatencyOperator.inputSchema = operator0.outputSchema.copy();
				calclatencyOperator.inputRate = operator0.outputRate;
				calclatencyOperator.inputName = operator0.outputName;

				calclatencyOperator.outputSchema = calclatencyOperator.inputSchema.copy();
				calclatencyOperator.outputRate = calclatencyOperator.inputRate;
				calclatencyOperator.outputName = "latency_" + currentNode.name + "_" + currentCount;
				
				operatorGraph.addVertex(calclatencyOperator, false);
				edgesToAdd.add(new DataFlow(operator0, calclatencyOperator));
				
				//Datarate Operator
				DatarateOperator datarateOperator = OperatorGenerator.generateDatarateOperator("datarate_" + currentNode.name + "_" + currentCount);

				datarateOperator.type = Type.BENCHMARK;
				
				datarateOperator.assignedID = operator0.assignedID;
				
				datarateOperator.inputSchema = calclatencyOperator.outputSchema.copy();
				datarateOperator.inputRate = calclatencyOperator.outputRate;
				datarateOperator.inputName = calclatencyOperator.outputName;

				datarateOperator.outputSchema = datarateOperator.inputSchema.copy();
				datarateOperator.outputRate = datarateOperator.inputRate;

				operatorGraph.addVertex(datarateOperator, false);
				edgesToAdd.add(new DataFlow(calclatencyOperator, datarateOperator));
				
				//MapOperator
				MapOperator mapOperator = new MapOperator();
				
				mapOperator.type = Type.BENCHMARK;
				
				mapOperator.assignedID = operator0.assignedID;
				
				mapOperator.inputSchema = datarateOperator.outputSchema.copy();
				mapOperator.inputRate = datarateOperator.outputRate;
				mapOperator.inputName = datarateOperator.outputName;
				
				List<String> epressions = mapOperator.inputSchema.toStringList();
				
				mapOperator.expressions = new ArrayList<>();
				
				for(int index2 = 0; index2 < epressions.size(); index2++)
				{
					mapOperator.expressions.add("'" + epressions.get(index2) + "'");
				}
				
				mapOperator.expressions.add("['TimeInterval.start', 'time_start']");
				mapOperator.expressions.add("['TimeInterval.end', 'time_end']");
				mapOperator.expressions.add("['ToInteger(elementAt(Datarate.Measurements[0],1))', 'bytes_sent']");
				mapOperator.expressions.add("['ToLong((Latency.lend-Latency.minlstart)/1000000)','minLatencyInMS']");
				mapOperator.expressions.add("['ToLong((Latency.lend-Latency.maxlstart)/1000000)','maxLatencyInMS']");
				
				mapOperator.outputSchema = mapOperator.inputSchema.copy();
				mapOperator.outputSchema.addColumn(new Column("time_start", Long.class));
				mapOperator.outputSchema.addColumn(new Column("time_end", Long.class));
				mapOperator.outputSchema.addColumn(new Column("bytes_sent", Integer.class));
				mapOperator.outputSchema.addColumn(new Column("minLatencyInMS", Long.class));
				mapOperator.outputSchema.addColumn(new Column("maxLatencyInMS", Long.class));
				mapOperator.outputRate = mapOperator.inputRate;
				mapOperator.outputName = "metadata_map_" + currentNode.name + "_" + currentCount;
				
				operatorGraph.addVertex(mapOperator, false);
				edgesToAdd.add(new DataFlow(datarateOperator, mapOperator));
				
				//DatabasesinkOperator
				DatabasesinkOperator databasesinkOperator = OperatorGenerator.generateDatabasesinkOperator("_result_" + currentNode.name + "_" + currentCount);
				
				databasesinkOperator.type = Type.BENCHMARK;
				
				databasesinkOperator.assignedID = operator0.assignedID;
				
				databasesinkOperator.inputSchema = mapOperator.outputSchema.copy();
				databasesinkOperator.inputRate = mapOperator.outputRate;
				databasesinkOperator.inputName = mapOperator.outputName;
				
				databasesinkOperator.outputSchema = databasesinkOperator.inputSchema.copy();
				databasesinkOperator.outputRate = databasesinkOperator.inputRate;
				databasesinkOperator.outputName = "sink_" + currentNode.name + "_" + currentCount;
				
				operatorGraph.addVertex(databasesinkOperator, false);
				edgesToAdd.add(new DataFlow(mapOperator, databasesinkOperator));
			}
		}
		
		operatorGraph.edges.removeAll(edgesToRemove);
		operatorGraph.edges.addAll(edgesToAdd);
		
		operatorGraph.setDataFlowDatarates();
		
		operatorGraph.setLabels();
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println("...Adding benchmark operators finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");
	}
	
	private static int getPerNodeCount(String nodeName)
	{
		Integer value = null;
		
		if(perNodeCount.containsKey(nodeName))
		{
			value = perNodeCount.get(nodeName);
		}
		else
		{
			value = 1;
		}
		
		perNodeCount.put(nodeName, value + 1);
		
		return value;
	}
}