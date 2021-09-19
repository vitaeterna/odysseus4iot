package odysseus4iot.placement;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.operator.AccessOperator;
import odysseus4iot.graph.operator.SenderOperator;
import odysseus4iot.graph.operator.gen.OperatorGenerator;
import odysseus4iot.graph.operator.meta.DataFlow;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.graph.operator.meta.OperatorGraph;
import odysseus4iot.graph.physical.meta.Node;
import odysseus4iot.graph.physical.meta.PhysicalGraph;

public class OperatorPlacementPartitioner
{
	public static void transformOperatorGraphToDistributed(OperatorGraph operatorGraph, PhysicalGraph physicalGraph)
	{
		List<Edge> edgesToAdd = new ArrayList<>();
		List<Edge> edgesToRemove = new ArrayList<>();
		
		Node node0 = null;
		Node node1 = null;
		
		Operator operator0 = null;
		Operator operator1 = null;
		
		DataFlow currentDataFlow = null;
		
		SenderOperator senderOperator = null;
		
		AccessOperator accessOperator = null;
		
		String[] socketSplit = null;
		
		for(int index = 0; index < operatorGraph.edges.size(); index++)
		{
			currentDataFlow = (DataFlow)operatorGraph.edges.get(index);
			
			operator0 = (Operator)currentDataFlow.vertex0;
			operator1 = (Operator)currentDataFlow.vertex1;
			
			if(operator0.assignedID.intValue() != operator1.assignedID.intValue())
			{
				//TODO: ___ set vertex ids correctly?
				//TODO: ___ set group
				//TODO: ___ filter redundant sender and access operators and edges
				edgesToRemove.add(currentDataFlow);
				
				node0 = physicalGraph.getNodeByID(operator0.assignedID.intValue());
				node1 = physicalGraph.getNodeByID(operator1.assignedID.intValue());
				
				socketSplit = node1.socket.split(":");
				
				senderOperator = OperatorGenerator.generateSenderOperator(socketSplit[0], Integer.parseInt(socketSplit[1]));
				
				senderOperator.assignedID = operator0.assignedID;
				
				senderOperator.inputSchema = operator0.outputSchema.copy();
				senderOperator.inputRate = operator0.outputRate;
				senderOperator.inputName = operator0.outputName;
				
				senderOperator.outputSchema = senderOperator.inputSchema.copy();
				senderOperator.outputRate = senderOperator.inputRate;
				senderOperator.outputName = senderOperator.inputName + "_sender";
				
				accessOperator = OperatorGenerator.generateAccessOperator(socketSplit[0], Integer.parseInt(socketSplit[1]));
				
				accessOperator.assignedID = operator1.assignedID;
				
				accessOperator.attributes = operator1.inputSchema.copy();
				
				accessOperator.outputSchema = operator1.inputSchema.copy();
				accessOperator.outputRate = operator1.inputRate;
				accessOperator.outputName = operator1.inputName;
				
				accessOperator.inputSchema = accessOperator.outputSchema.copy();
				accessOperator.inputRate = accessOperator.outputRate;
				accessOperator.inputName = null;
				
				operatorGraph.addVertex(senderOperator);
				operatorGraph.addVertex(accessOperator);
				
				edgesToAdd.add(new DataFlow(operator0, senderOperator));
				edgesToAdd.add(new DataFlow(senderOperator, accessOperator));
				edgesToAdd.add(new DataFlow(accessOperator, operator1));
			}
		}
		
		operatorGraph.edges.removeAll(edgesToRemove);
		operatorGraph.addAllEdges(edgesToAdd);
		
		operatorGraph.setLabels();
	}
	
	public static List<OperatorGraph> buildSubgraphs(OperatorGraph operatorGraph, PhysicalGraph physicalGraph)
	{
		List<OperatorGraph> subGraphs = new ArrayList<>();
		
		OperatorGraph subGraph = null;
		
		Operator operator0 = null;
		Operator operator1 = null;
		
		DataFlow currentDataFlow = null;
		
		Node currentNode = null;
		
		for(int index = 0; index < physicalGraph.vertices.size(); index++)
		{
			currentNode = (Node)physicalGraph.vertices.get(index);
			
			subGraph = new OperatorGraph();
			
			for(int index2 = 0; index2 < operatorGraph.vertices.size(); index++)
			{
				operator0 = (Operator)operatorGraph.vertices.get(index2);
				
				if(currentNode.id.intValue() == operator0.assignedID.intValue())
				{
					subGraph.addVertex(operator0);
				}
			}
			
			for(int index2 = 0; index2 < operatorGraph.edges.size(); index2++)
			{
				currentDataFlow = (DataFlow)operatorGraph.edges.get(index2);
				
				operator0 = (Operator)currentDataFlow.vertex0;
				operator1 = (Operator)currentDataFlow.vertex1;
				
				if(operator0.assignedID.intValue() == currentNode.id.intValue() && operator1.assignedID.intValue() == currentNode.id.intValue())
				{
					subGraph.addEdge(currentDataFlow);
				}
			}
			
			subGraphs.add(subGraph);
		}
		
		return subGraphs;
	}
}