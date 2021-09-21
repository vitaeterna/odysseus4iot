package odysseus4iot.placement;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Vertex;
import odysseus4iot.graph.operator.AccessOperator;
import odysseus4iot.graph.operator.MergeOperator;
import odysseus4iot.graph.operator.SenderOperator;
import odysseus4iot.graph.operator.gen.OperatorGenerator;
import odysseus4iot.graph.operator.meta.DataFlow;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.graph.operator.meta.OperatorGraph;
import odysseus4iot.graph.physical.meta.Node;
import odysseus4iot.graph.physical.meta.Node.Type;
import odysseus4iot.util.Util;
import odysseus4iot.graph.physical.meta.PhysicalGraph;

public class OperatorPlacementPartitioner
{
	public static void transformOperatorGraphToDistributed(OperatorGraph operatorGraph, PhysicalGraph physicalGraph)
	{
		Long startTimestamp = System.currentTimeMillis();
		Long endTimestamp = null;
		
		System.out.println("Converting Operator Graph " + operatorGraph.label + " to Distirbuted Graph...");
		
		operatorGraph.label += "_distributed";
		
		List<Edge> edgesToAdd = new ArrayList<>();
		List<Edge> edgesToRemove = new ArrayList<>();
		
		Node node1 = null;
		
		Operator operator0 = null;
		Operator operator1 = null;
		
		DataFlow currentDataFlow = null;
		
		SenderOperator senderOperator = null;
		
		AccessOperator accessOperator = null;
		
		String[] socketSplit = null;
		
		int counterAccessMerge = 0;
		
		for(int index = 0; index < operatorGraph.edges.size(); index++)
		{
			currentDataFlow = (DataFlow)operatorGraph.edges.get(index);
			
			operator0 = (Operator)currentDataFlow.vertex0;
			operator1 = (Operator)currentDataFlow.vertex1;
			
			if(operator0.assignedID.intValue() != operator1.assignedID.intValue())
			{
				//TODO: _ set group
				edgesToRemove.add(currentDataFlow);
				
				node1 = physicalGraph.getNodeByID(operator1.assignedID.intValue());
				
				socketSplit = node1.socket.split(":");
				
				if(operator0.assignedOperator1 == null)
				{
					senderOperator = OperatorGenerator.generateSenderOperator(socketSplit[0], Integer.parseInt(socketSplit[1]));
					
					senderOperator.assignedID = operator0.assignedID;
					
					senderOperator.inputSchema = operator0.outputSchema.copy();
					senderOperator.inputRate = operator0.outputRate;
					senderOperator.inputName = operator0.outputName;
					
					senderOperator.outputSchema = senderOperator.inputSchema.copy();
					senderOperator.outputRate = senderOperator.inputRate;
					senderOperator.outputName = senderOperator.inputName + "_sender";
					
					operator0.assignedOperator1 = senderOperator;
					
					operatorGraph.addVertex(senderOperator);
					
					edgesToAdd.add(new DataFlow(operator0, senderOperator));
				}
				else
				{
					senderOperator = (SenderOperator)operator0.assignedOperator1;
				}
				
				if(operator1.assignedOperator0 == null)
				{
					accessOperator = OperatorGenerator.generateAccessOperator(socketSplit[0], Integer.parseInt(socketSplit[1]));
					
					accessOperator.assignedID = operator1.assignedID;
					
					accessOperator.attributes = operator1.inputSchema.copy();
					
					accessOperator.outputSchema = operator1.inputSchema.copy();
					accessOperator.outputRate = operator1.inputRate;
					
					if(operator1.inputName == null)
					{
						counterAccessMerge++;
						
						MergeOperator mergeOperator = (MergeOperator)operator1;
						
						accessOperator.outputName = "access_merge_" + counterAccessMerge;
						
						mergeOperator.inputStreams.clear();
						mergeOperator.inputStreams.add(accessOperator.outputName);
					}
					else
					{
						accessOperator.outputName = operator1.inputName;
					}
					
					accessOperator.inputSchema = accessOperator.outputSchema.copy();
					accessOperator.inputRate = accessOperator.outputRate;
					accessOperator.inputName = null;
					
					operator1.assignedOperator0 = accessOperator;
					
					operatorGraph.addVertex(accessOperator);
					
					edgesToAdd.add(new DataFlow(accessOperator, operator1));
				}
				else
				{
					accessOperator = (AccessOperator)operator1.assignedOperator0;
				}
				
				edgesToAdd.add(new DataFlow(senderOperator, accessOperator));
			}
		}
		
		operatorGraph.edges.removeAll(edgesToRemove);
		operatorGraph.addAllEdges(edgesToAdd);
		
		operatorGraph.setControlFlowDatarates();
		
		operatorGraph.setLabels();
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println("...Converting to Distirbuted Graph finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");
	}
	
	public static List<OperatorGraph> buildSubgraphs(OperatorGraph operatorGraph, PhysicalGraph physicalGraph)
	{
		Long startTimestamp = System.currentTimeMillis();
		Long endTimestamp = null;
		
		System.out.println("Generation of Distributed Operator Subgraphs...");
		
		List<OperatorGraph> subGraphs = new ArrayList<>();
		
		OperatorGraph subGraph = null;
		OperatorGraph subGraph2 = null;
		
		Operator operator0 = null;
		Operator operator1 = null;
		
		DataFlow currentDataFlow = null;
		
		Node currentNode = null;
		
		for(int index = 0; index < physicalGraph.vertices.size(); index++)
		{
			currentNode = (Node)physicalGraph.vertices.get(index);
			
			subGraph = new OperatorGraph(currentNode.name);
			
			for(int index2 = 0; index2 < operatorGraph.vertices.size(); index2++)
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
			
			if(currentNode.type.equals(Type.EDGE))
			{
				List<Vertex> startingVertices = subGraph.getStartingVertices();
				
				for(int index2 = 0; index2 < startingVertices.size(); index2++)
				{
					operator0 = (Operator)startingVertices.get(index2);
					
					subGraph2 = new OperatorGraph(operator0.outputName);
					
					subGraph2.addAllVertices(subGraph.getVerticesBreadthFirst(operator0));
					
					for(int index3 = 0; index3 < subGraph.edges.size(); index3++)
					{
						currentDataFlow = (DataFlow)subGraph.edges.get(index3);
						
						operator0 = (Operator)currentDataFlow.vertex0;
						operator1 = (Operator)currentDataFlow.vertex1;
						
						if(subGraph2.vertices.contains(operator0) && subGraph2.vertices.contains(operator1))
						{
							subGraph2.addEdge(currentDataFlow);
						}
					}
					
					if(!subGraph2.isEmpty())
					{
						subGraphs.add(subGraph2);
					}
				}
			}
			else
			{
				if(!subGraph.isEmpty())
				{
					subGraphs.add(subGraph);
				}
			}
		}
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println(subGraphs.size() + " subgraphs generated!");
		
		System.out.println("...Generation of Distributed Operator Subgraphs finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");

		return subGraphs;
	}
}