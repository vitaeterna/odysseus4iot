package odysseus4iot.graph.physical.gen;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Graph;
import odysseus4iot.graph.physical.meta.Connection;
import odysseus4iot.graph.physical.meta.Node;
import odysseus4iot.graph.physical.meta.Node.Type;
import odysseus4iot.util.Util;

public class PhysicalGraphGenerator
{
	public static Graph generatePhysicalraph(List<String> nodeNames, List<String> nodeSockets, List<String> nodeTypes, List<String> nodeCPUCaps, List<String> nodeMemCaps, List<String> edges, List<String> edgeRateCaps)
	{
		Graph graph = new Graph();
		
		List<Node> nodes = new ArrayList<>();
		
		Node node = null;
		
		String currentNodeName = null;
		String currentNodeSocket = null;
		Type currentNodeType = null;
		Integer currentNodeCPUCap = null;
		Integer currentNodeMemCap = null;
		
		for(int index = 0; index < nodeNames.size(); index++)
		{
			currentNodeName = nodeNames.get(index);
			currentNodeSocket = nodeSockets.get(index);
			currentNodeType = Type.valueOf(nodeTypes.get(index).toUpperCase());
			currentNodeCPUCap = Integer.parseInt(nodeCPUCaps.get(index));
			currentNodeMemCap = Integer.parseInt(nodeMemCaps.get(index));
			
			node = NodeGenerator.generateNode(currentNodeName, currentNodeSocket, currentNodeType, currentNodeCPUCap, currentNodeMemCap);
			
			node.label = node.id + "_" + node.name + "\n" + node.type + "\n" + node.cpuCapacity + "/" + node.memCapacity;
			
			nodes.add(node);
			
			graph.addVertex(node);
		}
		
		List<Connection> connections = new ArrayList<>();
		
		Connection connection = null;
		
		String currentEdge = null;
		Integer currentEdgeRateCap = null;
		
		String[] currentEdgeSplit = null;
		
		for(int index = 0; index < edges.size(); index++)
		{
			currentEdge = edges.get(index);
			currentEdgeRateCap = Integer.parseInt(edgeRateCaps.get(index));
			
			currentEdgeSplit = currentEdge.split("~");
			
			if(currentEdgeSplit.length != 2)
			{
				System.err.println("Invalid edge syntax on '" + currentEdge + "'. Please use 'x~y' where x and y are node ids.");
			
				System.exit(0);
			}
			
			connection = new Connection(graph.getVertexByID(Integer.parseInt(currentEdgeSplit[0])), graph.getVertexByID(Integer.parseInt(currentEdgeSplit[1])), currentEdgeRateCap);
			
			connection.label = Util.formatNumber((double)currentEdgeRateCap);
			
			connections.add(connection);
			
			graph.addEdge(connection);
		}
		
		return graph;
	}
}