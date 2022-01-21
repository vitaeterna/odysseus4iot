package odysseus4iot.placement;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.operator.CalcLatencyOperator;
import odysseus4iot.graph.operator.DatabasesinkOperator;
import odysseus4iot.graph.operator.DatarateOperator;
import odysseus4iot.graph.operator.MapOperator;
import odysseus4iot.graph.operator.SenderOperator;
import odysseus4iot.graph.operator.gen.OperatorGenerator;
import odysseus4iot.graph.operator.meta.Column;
import odysseus4iot.graph.operator.meta.DataFlow;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.graph.operator.meta.OperatorGraph;
import odysseus4iot.graph.operator.meta.Operator.Type;
import odysseus4iot.graph.physical.meta.Node;
import odysseus4iot.graph.physical.meta.PhysicalGraph;
import odysseus4iot.util.Util;

public class OperatorPlacementBenchmark
{
	/*public static void addBenchmarkOperators(OperatorGraph operatorGraph, PhysicalGraph physicalGraph)
	{
		Long startTimestamp = System.currentTimeMillis();
		Long endTimestamp = null;
		
		System.out.println("Adding benchmark operators to Operator Graph " + operatorGraph.label + "...");
		
		operatorGraph.label += "_benchmark";
		
		SenderOperator currentSenderOperator = null;
		
		DataFlow currentDataFlow = null;
		
		List<Vertex> senderOperators = operatorGraph.getVerticesByType(SenderOperator.class);
		
		List<DataFlow> edgesToAdd = new ArrayList<>();
		List<DataFlow> edgesToRemove = new ArrayList<>();
		
		for(int index = 0; index < senderOperators.size(); index++)
		{
			currentSenderOperator = (SenderOperator)senderOperators.get(index);
			
			DatarateOperator datarateOperator = OperatorGenerator.generateDatarateOperator(physicalGraph.getNodeByID(currentSenderOperator.assignedID).name);
			
			datarateOperator.assignedID = currentSenderOperator.assignedID;
			
			datarateOperator.inputSchema = currentSenderOperator.inputSchema.copy();
			datarateOperator.inputRate = currentSenderOperator.inputRate;
			datarateOperator.inputName = currentSenderOperator.inputName;
			
			datarateOperator.outputSchema = datarateOperator.inputSchema.copy();
			datarateOperator.outputRate = datarateOperator.inputRate;
			
			currentSenderOperator.inputName = datarateOperator.outputName;
			
			operatorGraph.addVertex(datarateOperator);
			
			int senderInputCount = 0;
			
			for(int index2 = 0; index2 < operatorGraph.edges.size(); index2++)
			{
				currentDataFlow = (DataFlow)operatorGraph.edges.get(index2);
				
				if(currentDataFlow.vertex1 == currentSenderOperator)
				{
					senderInputCount++;
					
					edgesToRemove.add(currentDataFlow);
					
					edgesToAdd.add(new DataFlow(currentDataFlow.vertex0, datarateOperator));
					edgesToAdd.add(new DataFlow(datarateOperator, currentSenderOperator));
				}
			}
			
			if(senderInputCount != 1)
			{
				System.err.println("There is something strange in the senderhood");
				
				System.exit(0);
			}
		}
		
		operatorGraph.edges.removeAll(edgesToRemove);
		operatorGraph.edges.addAll(edgesToAdd);
		
		operatorGraph.setDataFlowDatarates();
		
		operatorGraph.setLabels();
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println("...Adding benchmark operators finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");
	}*/
	
	public static void addBenchmarkOperators(OperatorGraph operatorGraph, PhysicalGraph physicalGraph)
	{
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
		
		for(int index = 0; index < operatorGraph.edges.size(); index++)
		{
			currentDataFlow = (DataFlow)operatorGraph.edges.get(index);
			
			operator0 = (Operator)currentDataFlow.vertex0;
			operator1 = (Operator)currentDataFlow.vertex1;
			
			currentNode = physicalGraph.getNodeByID(operator0.assignedID);
			
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
					calclatencyOperator.outputName = "latency_" + currentNode.name + "_" + (DatarateOperator.getCurrentDatarateCount() + 1);
					
					operatorGraph.addVertex(calclatencyOperator, false);
					edgesToAdd.add(new DataFlow(operator0, calclatencyOperator));
					
					//Datarate Operator
					DatarateOperator datarateOperator = OperatorGenerator.generateDatarateOperator(currentNode.name);

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
					mapOperator.outputName = "datarate_map_" + currentNode.name + "_" + DatarateOperator.getCurrentDatarateCount();
					
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
		
		for(int index = 0; index < operatorGraph.edges.size(); index++)
		{
			currentDataFlow = (DataFlow)operatorGraph.edges.get(index);
			
			operator0 = (Operator)currentDataFlow.vertex0;
			operator1 = (Operator)currentDataFlow.vertex1;
			
			currentNode = physicalGraph.getNodeByID(operator0.assignedID);
			
			if(operator0.assignedID.intValue() != operator1.assignedID.intValue())
			{
				if(operator0 instanceof SenderOperator)
				{
					operator0 = (Operator)operatorGraph.getPredecessors(operator0).get(0);
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
				calclatencyOperator.outputName = "latency_" + currentNode.name + "_" + (DatarateOperator.getCurrentDatarateCount() + 1);
				
				operatorGraph.addVertex(calclatencyOperator, false);
				operatorGraph.addEdge(new DataFlow(operator0, calclatencyOperator));
				
				//Datarate Operator
				DatarateOperator datarateOperator = OperatorGenerator.generateDatarateOperator(currentNode.name);

				datarateOperator.type = Type.BENCHMARK;
				
				datarateOperator.assignedID = operator0.assignedID;
				
				datarateOperator.inputSchema = calclatencyOperator.outputSchema.copy();
				datarateOperator.inputRate = calclatencyOperator.outputRate;
				datarateOperator.inputName = calclatencyOperator.outputName;

				datarateOperator.outputSchema = datarateOperator.inputSchema.copy();
				datarateOperator.outputRate = datarateOperator.inputRate;

				operatorGraph.addVertex(datarateOperator, false);
				operatorGraph.addEdge(new DataFlow(calclatencyOperator, datarateOperator));
				
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
				mapOperator.outputName = "datarate_map_" + currentNode.name + "_" + DatarateOperator.getCurrentDatarateCount();
				
				operatorGraph.addVertex(mapOperator, false);
				operatorGraph.addEdge(new DataFlow(datarateOperator, mapOperator));
				
				//DatabasesinkOperator
				DatabasesinkOperator databasesinkOperator = OperatorGenerator.generateDatabasesinkOperator("_result_" + currentNode.name + "_" + DatarateOperator.getCurrentDatarateCount());
				
				databasesinkOperator.type = Type.BENCHMARK;
				
				databasesinkOperator.assignedID = operator0.assignedID;
				
				databasesinkOperator.inputSchema = mapOperator.outputSchema.copy();
				databasesinkOperator.inputRate = mapOperator.outputRate;
				databasesinkOperator.inputName = mapOperator.outputName;
				
				databasesinkOperator.outputSchema = databasesinkOperator.inputSchema.copy();
				databasesinkOperator.outputRate = databasesinkOperator.inputRate;
				databasesinkOperator.outputName = "datarate_sink_" + currentNode.name + "_" + DatarateOperator.getCurrentDatarateCount();
				
				operatorGraph.addVertex(databasesinkOperator, false);
				operatorGraph.addEdge(new DataFlow(mapOperator, databasesinkOperator));
			}
		}
		
		operatorGraph.setDataFlowDatarates();
		
		operatorGraph.setLabels();
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println("...Adding benchmark operators finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");
	}
}