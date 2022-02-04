package odysseus4iot.placement;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Vertex;
import odysseus4iot.graph.operator.AccessOperator;
import odysseus4iot.graph.operator.DatabasesinkOperator;
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

/**
 * The {@code OperatorPlacementPartitioning} provides methods to partition global queries given as operator graph to partial queries (operator subgraphs).
 * 
 * @author Michael Sünkel
 */
public class OperatorPlacementPartitioning
{
	/**
	 * Regarding the given mapping of operators to nodes the operator graph is enriched by sender and access operators.
	 * 
	 * @param operatorGraph
	 * @param physicalGraph
	 */
	public static void transformOperatorGraphToDistributed(OperatorGraph operatorGraph, PhysicalGraph physicalGraph)
	{
		Long startTimestamp = System.currentTimeMillis();
		Long endTimestamp = null;
		
		System.out.println("Converting Operator Graph " + operatorGraph.label + " to Distributed Graph...");
		
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
		
		int currentPort = 9200;
		
		boolean senderOperatorToSameNodeExists = false;
		
		for(int index = 0; index < operatorGraph.edges.size(); index++)
		{
			currentDataFlow = (DataFlow)operatorGraph.edges.get(index);
			
			operator0 = (Operator)currentDataFlow.vertex0;
			operator1 = (Operator)currentDataFlow.vertex1;
			
			if(operator0.assignedID.intValue() != operator1.assignedID.intValue())
			{
				edgesToRemove.add(currentDataFlow);
				
				node1 = physicalGraph.getNodeByID(operator1.assignedID.intValue());
				
				socketSplit = node1.socket.split(":");
				
				senderOperator = null;
				
				for(int index2 = 0; index2 < operator0.assignedOperators1.size(); index2++)
				{
					if(operator0.assignedOperators1.get(index2).assignedOperators1.get(0).assignedID.intValue() == operator1.assignedID.intValue())
					{
						//Case 3.2
						senderOperator = (SenderOperator)operator0.assignedOperators1.get(index2);
						
						senderOperatorToSameNodeExists = true;
						
						break;
					}
				}
				
				if(senderOperator == null)
				{
					senderOperator = OperatorGenerator.generateSenderOperator(socketSplit[0], Integer.parseInt(socketSplit[1]));
					
					senderOperator.group = operator0.group + 1;
					
					senderOperator.assignedID = operator0.assignedID;
					
					senderOperator.inputSchema = operator0.outputSchema.copy();
					senderOperator.inputRate = operator0.outputRate;
					senderOperator.inputName = operator0.outputName;
					
					senderOperator.outputSchema = senderOperator.inputSchema.copy();
					senderOperator.outputRate = senderOperator.inputRate;
					senderOperator.outputName = "sender_" + SenderOperator.getNextSenderCount();
					
					operator0.assignedOperators1.add(senderOperator);
					
					operatorGraph.addVertex(senderOperator, false);
					
					edgesToAdd.add(new DataFlow(operator0, senderOperator));
				}
				
				if(senderOperatorToSameNodeExists)
				{
					//Case 3.2
					accessOperator = (AccessOperator)senderOperator.assignedOperators1.get(0);
					
					operator1.inputName = accessOperator.outputName;
					
					operator1.assignedOperators0.add(accessOperator);
					
					edgesToAdd.add(new DataFlow(accessOperator, operator1));
				}
				else
				{
					if(operator1.assignedOperators0.isEmpty())
					{
						accessOperator = OperatorGenerator.generateAccessOperator(socketSplit[0], ++currentPort);
						
						node1.ports.add(currentPort);
						
						accessOperator.group = operator1.group - 1;
						
						accessOperator.assignedID = operator1.assignedID;
						
						accessOperator.attributes = operator1.inputSchema.copy();
						
						accessOperator.outputSchema = operator1.inputSchema.copy();
						accessOperator.outputRate = operator1.inputRate;
						
						if(operator1.inputName == null)
						{
							counterAccessMerge++;
							
							MergeOperator mergeOperator = (MergeOperator)operator1;
							
							accessOperator.outputName = "access_merge_" + counterAccessMerge;
							
							mergeOperator.inputStreams.remove(operator0.outputName);
							mergeOperator.inputStreams.add(accessOperator.outputName);
						}
						else
						{
							accessOperator.outputName = "access_" + AccessOperator.getNextAccessCount();
							
							operator1.inputName = accessOperator.outputName;
						}
						
						accessOperator.inputSchema = accessOperator.outputSchema.copy();
						accessOperator.inputRate = accessOperator.outputRate;
						accessOperator.inputName = null;
						
						operator1.assignedOperators0.add(accessOperator);
						
						operatorGraph.addVertex(accessOperator, false);
						
						edgesToAdd.add(new DataFlow(accessOperator, operator1));
					}
					else
					{
						//Case 2.1
						accessOperator = (AccessOperator)operator1.assignedOperators0.get(0);
					}
				}
				
				if(!senderOperator.assignedOperators1.contains(accessOperator))
				{
					senderOperator.assignedOperators1.add(accessOperator);
					
					senderOperator.port = accessOperator.port;
					
					edgesToAdd.add(new DataFlow(senderOperator, accessOperator));
				}
			}
		}
		
		operatorGraph.edges.removeAll(edgesToRemove);
		operatorGraph.addAllEdges(edgesToAdd);
		
		operatorGraph.setDataFlowDatarates();
		
		operatorGraph.setLabels();
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println("...Converting to Distributed Graph finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");
	}
	
	/**
	 * Splits an operator graph into subgraphs regarding the given mapping of operators to nodes.
	 * 
	 * @param operatorGraph
	 * @param physicalGraph
	 * @return
	 */
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
		
		int edgeNodes = 0;
		
		for(int index = 0; index < physicalGraph.vertices.size(); index++)
		{
			currentNode = (Node)physicalGraph.vertices.get(index);
			
			subGraph = new OperatorGraph(currentNode.name);
			
			for(int index2 = 0; index2 < operatorGraph.vertices.size(); index2++)
			{
				operator0 = (Operator)operatorGraph.vertices.get(index2);
				
				if(currentNode.id.intValue() == operator0.assignedID.intValue())
				{
					subGraph.addVertex(operator0.copy(), true);
				}
			}
			
			for(int index2 = 0; index2 < operatorGraph.edges.size(); index2++)
			{
				currentDataFlow = (DataFlow)operatorGraph.edges.get(index2);
				
				operator0 = (Operator)currentDataFlow.vertex0;
				operator1 = (Operator)currentDataFlow.vertex1;
				
				if(operator0.assignedID.intValue() == currentNode.id.intValue() && operator1.assignedID.intValue() == currentNode.id.intValue())
				{
					currentDataFlow = currentDataFlow.copy();
					
					currentDataFlow.vertex0 = subGraph.getVertexByID(operator0.id);
					currentDataFlow.vertex1 = subGraph.getVertexByID(operator1.id);
					
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
					
					List<Vertex> breadthFirstVertices = subGraph.getVerticesBreadthFirst(operator0);
					
					for(int index3 = 0; index3 < breadthFirstVertices.size(); index3++)
					{
						operator1 = ((Operator)breadthFirstVertices.get(index3)).copy();
						
						if(operator1 instanceof DatabasesinkOperator)
						{
							((DatabasesinkOperator)operator1).table = ((DatabasesinkOperator)operator1).table.replace("node1", subGraph2.label);
						}
						
						subGraph2.addVertex(operator1, true);
					}
					
					for(int index3 = 0; index3 < subGraph.edges.size(); index3++)
					{
						currentDataFlow = (DataFlow)subGraph.edges.get(index3);
						
						operator0 = (Operator)currentDataFlow.vertex0;
						operator1 = (Operator)currentDataFlow.vertex1;
						
						if(subGraph2.getVertexByID(operator0.id) != null && subGraph2.getVertexByID(operator1.id) != null)
						{
							currentDataFlow = currentDataFlow.copy();
							
							currentDataFlow.vertex0 = subGraph2.getVertexByID(operator0.id);
							currentDataFlow.vertex1 = subGraph2.getVertexByID(operator1.id);
							
							subGraph2.addEdge(currentDataFlow);
						}
					}
					
					subGraph2.removeSuperfluousOperators();
					
					if(!subGraph2.isEmpty())
					{
						subGraph2.socket = currentNode.socket.split(":")[0] + ":" + (Integer.parseInt(currentNode.socket.split(":")[1]) + edgeNodes++);
						
						subGraphs.add(subGraph2);
					}
				}
			}
			else
			{
				subGraph.removeSuperfluousOperators();
				
				if(!subGraph.isEmpty())
				{
					subGraph.socket = currentNode.socket;
					
					subGraphs.add(subGraph);
				}
			}
		}
		
		for(int index = 0; index < subGraphs.size(); index++)
		{
			subGraphs.get(index).setDataFlowDatarates();
			subGraphs.get(index).setLabels();
		}
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println(subGraphs.size() + " subgraphs generated!");
		
		System.out.println("...Generation of Distributed Operator Subgraphs finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");

		return subGraphs;
	}
}