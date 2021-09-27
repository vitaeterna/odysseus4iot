package odysseus4iot.placement;

import odysseus4iot.graph.operator.DatabasesinkOperator;
import odysseus4iot.graph.operator.DatarateOperator;
import odysseus4iot.graph.operator.MapOperator;
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
		
		operatorGraph.setControlFlowDatarates();
		
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
		
		for(int index = 0; index < operatorGraph.edges.size(); index++)
		{
			currentDataFlow = (DataFlow)operatorGraph.edges.get(index);
			
			operator0 = (Operator)currentDataFlow.vertex0;
			operator1 = (Operator)currentDataFlow.vertex1;
			
			currentNode = physicalGraph.getNodeByID(operator0.assignedID);
			
			if(operator0.assignedID.intValue() != operator1.assignedID.intValue())
			{
				//Datarate Operator
				DatarateOperator datarateOperator = OperatorGenerator.generateDatarateOperator(currentNode.name);

				datarateOperator.type = Type.BENCHMARK;
				
				datarateOperator.assignedID = operator0.assignedID;
				
				datarateOperator.inputSchema = operator0.outputSchema.copy();
				datarateOperator.inputRate = operator0.outputRate;
				datarateOperator.inputName = operator0.outputName;

				datarateOperator.outputSchema = datarateOperator.inputSchema.copy();
				datarateOperator.outputRate = datarateOperator.inputRate;

				operatorGraph.addVertex(datarateOperator);
				operatorGraph.addEdge(new DataFlow(operator0, datarateOperator));
				
				//MapOperator
				MapOperator mapOperator = new MapOperator();
				
				mapOperator.type = Type.BENCHMARK;
				
				mapOperator.assignedID = operator0.assignedID;
				
				mapOperator.inputSchema = datarateOperator.outputSchema.copy();
				mapOperator.inputRate = datarateOperator.outputRate;
				mapOperator.inputName = datarateOperator.outputName;
				
				mapOperator.expressions = mapOperator.inputSchema.toStringList();
				mapOperator.expressions.add("['TimeInterval.start', 'time_start']");
				mapOperator.expressions.add("['TimeInterval.end', 'time_end']");
				mapOperator.expressions.add("['ToInteger(elementAt(Datarate.Measurements[0],1))', 'bytes_sent']");
				
				mapOperator.outputSchema = mapOperator.inputSchema.copy();
				mapOperator.outputSchema.addColumn(new Column("time_start", Long.class));
				mapOperator.outputSchema.addColumn(new Column("time_end", Long.class));
				mapOperator.outputSchema.addColumn(new Column("bytes_sent", Integer.class));
				mapOperator.outputRate = mapOperator.inputRate;
				mapOperator.outputName = "datarate_map_" + currentNode.name + "_" + DatarateOperator.getCurrentDatarateCount();
				
				operatorGraph.addVertex(mapOperator);
				operatorGraph.addEdge(new DataFlow(datarateOperator, mapOperator));
				
				//DatabasesinkOperator
				DatabasesinkOperator databasesinkOperator = OperatorGenerator.generateDatabasesinkOperator(currentNode.name + "_" + DatarateOperator.getCurrentDatarateCount());
				
				databasesinkOperator.type = Type.BENCHMARK;
				
				databasesinkOperator.assignedID = operator0.assignedID;
				
				databasesinkOperator.inputSchema = mapOperator.outputSchema.copy();
				databasesinkOperator.inputRate = mapOperator.outputRate;
				databasesinkOperator.inputName = mapOperator.outputName;
				
				databasesinkOperator.outputSchema = databasesinkOperator.inputSchema.copy();
				databasesinkOperator.outputRate = databasesinkOperator.inputRate;
				databasesinkOperator.outputName = "datarate_sink_" + currentNode.name + "_" + DatarateOperator.getCurrentDatarateCount();
				
				operatorGraph.addVertex(databasesinkOperator);
				operatorGraph.addEdge(new DataFlow(mapOperator, databasesinkOperator));
			}
		}
		
		operatorGraph.setControlFlowDatarates();
		
		operatorGraph.setLabels();
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println("...Adding benchmark operators finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");
	}
}