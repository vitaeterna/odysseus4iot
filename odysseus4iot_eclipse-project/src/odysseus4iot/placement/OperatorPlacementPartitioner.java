package odysseus4iot.placement;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Vertex;
import odysseus4iot.graph.operator.AccessOperator;
import odysseus4iot.graph.operator.DatarateOperator;
import odysseus4iot.graph.operator.MergeOperator;
import odysseus4iot.graph.operator.SenderOperator;
import odysseus4iot.graph.operator.gen.OperatorGenerator;
import odysseus4iot.graph.operator.meta.DataFlow;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.graph.operator.meta.OperatorGraph;
import odysseus4iot.graph.physical.meta.Node;
import odysseus4iot.util.Util;
import odysseus4iot.graph.physical.meta.PhysicalGraph;

public class OperatorPlacementPartitioner
{
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
					senderOperator.outputName = "sender_" + SenderOperator.getNextSenderCount();
					
					//operator0.assignedOperator1 = senderOperator;
					
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
						accessOperator.outputName = "access_" + AccessOperator.getNextAccessCount();
						
						operator1.inputName = accessOperator.outputName;
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
		
		System.out.println("...Converting to Distributed Graph finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");
	}
	
	public static void addBenchmarkOperators(OperatorGraph operatorGraph, PhysicalGraph physicalGraph)
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
	}
	
	public static void addBenchmarkOperatorsSingleNode(OperatorGraph operatorGraph, PhysicalGraph physicalGraph)
	{
		Long startTimestamp = System.currentTimeMillis();
		Long endTimestamp = null;
		
		System.out.println("Adding benchmark operators to Operator Graph " + operatorGraph.label + "...");
		
		operatorGraph.label += "_benchmark";
		
		List<Edge> edgesToAdd = new ArrayList<>();
		List<Edge> edgesToRemove = new ArrayList<>();
		
		Operator operator0 = null;
		Operator operator1 = null;
		
		DataFlow currentDataFlow = null;
		
		for(int index = 0; index < operatorGraph.edges.size(); index++)
		{
			currentDataFlow = (DataFlow)operatorGraph.edges.get(index);
			
			operator0 = (Operator)currentDataFlow.vertex0;
			operator1 = (Operator)currentDataFlow.vertex1;
			
			if(operator0.assignedID.intValue() != operator1.assignedID.intValue())
			{
				edgesToRemove.add(currentDataFlow);

				DatarateOperator datarateOperator = null;
				
				if(operator0.assignedOperator1 == null)
				{
					datarateOperator = OperatorGenerator.generateDatarateOperator(physicalGraph.getNodeByID(operator0.assignedID).name);
					
					datarateOperator.assignedID = operator1.assignedID;
					
					datarateOperator.inputSchema = operator0.outputSchema.copy();
					datarateOperator.inputRate = operator0.outputRate;
					datarateOperator.inputName = operator0.outputName;
					
					datarateOperator.outputSchema = datarateOperator.inputSchema.copy();
					datarateOperator.outputRate = datarateOperator.inputRate;
					
					if(operator1.inputName == null)
					{
						MergeOperator mergeOperator = (MergeOperator)operator1;
						
						mergeOperator.inputStreams.remove(operator0.outputName);
						mergeOperator.inputStreams.add(datarateOperator.outputName);
					}
					else
					{
						operator1.inputName = datarateOperator.outputName;
					}
					
					operator0.assignedOperator1 = datarateOperator;
					
					operatorGraph.addVertex(datarateOperator);
					
					edgesToAdd.add(new DataFlow(operator0, datarateOperator));
				}
				else
				{
					datarateOperator = (DatarateOperator)operator0.assignedOperator1;
					
					if(operator1.inputName == null)
					{
						MergeOperator mergeOperator = (MergeOperator)operator1;
						
						mergeOperator.inputStreams.remove(operator0.outputName);
						mergeOperator.inputStreams.add(datarateOperator.outputName);
					}
					else
					{
						operator1.inputName = datarateOperator.outputName;
					}
				}
				
				edgesToAdd.add(new DataFlow(datarateOperator, operator1));
			}
		}
		
		operatorGraph.edges.removeAll(edgesToRemove);
		operatorGraph.addAllEdges(edgesToAdd);
		
		operatorGraph.setControlFlowDatarates();
		
		operatorGraph.setLabels();
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println("...Adding benchmark operators finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");
	}
	
	public static List<OperatorGraph> buildSubgraphs(OperatorGraph operatorGraph, PhysicalGraph physicalGraph)
	{
		Long startTimestamp = System.currentTimeMillis();
		Long endTimestamp = null;
		
		System.out.println("Generation of Distributed Operator Subgraphs...");
		
		List<OperatorGraph> subGraphs = new ArrayList<>();
		
		OperatorGraph subGraph = null;
		//OperatorGraph subGraph2 = null;
		
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
			
			/*if(currentNode.type.equals(Type.EDGE))
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
					
					List<Vertex> mergeOperators = subGraph2.getVerticesByType(MergeOperator.class);
					
					List<Edge> inputEdges = null;
					List<Edge> inputEdgesNew = null;
					List<Edge> outputEdges = null;
					List<Edge> outputEdgesNew = null;
					
					MergeOperator mergeOperator = null;
					MergeOperator mergeOperatorNew = null;
					
					//TODO: ___ instead of copying the merge nodes and modify them they can just be removed!
					for(int index3 = 0; index3 < mergeOperators.size(); index3++)
					{
						mergeOperator = (MergeOperator)mergeOperators.get(index3);
						
						inputEdges = subGraph2.getInputEdges(mergeOperator);
						outputEdges = subGraph2.getOutputEdges(mergeOperator);
						
						mergeOperatorNew = mergeOperator.copy();
						
						inputEdgesNew = new ArrayList<>();
						
						for(int index4 = 0; index4 < inputEdges.size(); index4++)
						{
							currentDataFlow = ((DataFlow)inputEdges.get(index4)).copy();
							
							currentDataFlow.vertex1 = mergeOperatorNew;
							
							inputEdgesNew.add(currentDataFlow);
						}
						
						outputEdgesNew = new ArrayList<>();
						
						for(int index4 = 0; index4 < outputEdges.size(); index4++)
						{
							currentDataFlow = ((DataFlow)outputEdges.get(index4)).copy();
							
							currentDataFlow.vertex0 = mergeOperatorNew;
							
							outputEdgesNew.add(currentDataFlow);
						}
						
						subGraph2.vertices.remove(mergeOperator);
						subGraph2.addVertex(mergeOperatorNew);
						subGraph2.edges.removeAll(inputEdges);
						subGraph2.addAllEdges(inputEdgesNew);
						subGraph2.edges.removeAll(outputEdges);
						subGraph2.addAllEdges(outputEdgesNew);
						
						if(mergeOperatorNew.inputStreams.contains(subGraph2.label))
						{
							mergeOperatorNew.inputStreams.clear();
							mergeOperatorNew.inputStreams.add(subGraph2.label);
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
			}*/
			
			if(!subGraph.isEmpty())
			{
				subGraphs.add(subGraph);
			}
		}
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println(subGraphs.size() + " subgraphs generated!");
		
		System.out.println("...Generation of Distributed Operator Subgraphs finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");

		return subGraphs;
	}
}