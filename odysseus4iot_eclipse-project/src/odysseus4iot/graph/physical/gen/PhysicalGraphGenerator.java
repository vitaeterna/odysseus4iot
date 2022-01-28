package odysseus4iot.graph.physical.gen;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.physical.meta.Connection;
import odysseus4iot.graph.physical.meta.Node;
import odysseus4iot.graph.physical.meta.Node.Type;
import odysseus4iot.graph.physical.meta.PhysicalGraph;
import odysseus4iot.util.Util;

public class PhysicalGraphGenerator
{
	public static PhysicalGraph generatePhysicalGraph(List<String> nodeNames, List<String> nodeSockets, List<String> nodeTypes, List<String> nodeCPUCaps, List<String> nodeMemCaps, List<String> edges, List<String> edgeRateCaps, List<String> edgeDelays)
	{
		Long startTimestamp = System.currentTimeMillis();
		Long endTimestamp = null;
		
		System.out.println("Generation of Physical Graph started...");
		
		PhysicalGraph physicalGraph = new PhysicalGraph("physical");
		
		List<Node> nodes = new ArrayList<>();
		
		Node node = null;
		
		String currentNodeName = null;
		String currentNodeSocket = null;
		Type currentNodeType = null;
		Long currentNodeCPUCap = null;
		Long currentNodeMemCap = null;
		
		for(int index = 0; index < nodeNames.size(); index++)
		{
			currentNodeName = nodeNames.get(index);
			currentNodeSocket = nodeSockets.get(index);
			currentNodeType = Type.valueOf(nodeTypes.get(index).toUpperCase());
			currentNodeCPUCap = Long.parseLong(nodeCPUCaps.get(index));
			currentNodeMemCap = Long.parseLong(nodeMemCaps.get(index));
			
			node = NodeGenerator.generateNode(currentNodeName, currentNodeSocket, currentNodeType, currentNodeCPUCap, currentNodeMemCap);
			
			nodes.add(node);
			
			physicalGraph.addVertex(node, false);
		}
		
		List<Connection> connections = new ArrayList<>();
		
		Connection connection = null;
		
		String currentEdge = null;
		Integer currentEdgeRateCap = null;
		Integer currentEdgeDelay = null;
		
		String[] currentEdgeSplit = null;
		
		for(int index = 0; index < edges.size(); index++)
		{
			currentEdge = edges.get(index);
			currentEdgeRateCap = Integer.parseInt(edgeRateCaps.get(index));
			currentEdgeDelay = Integer.parseInt(edgeDelays.get(index));
			
			currentEdgeSplit = currentEdge.split("~");
			
			if(currentEdgeSplit.length != 2)
			{
				System.err.println("Invalid edge syntax on '" + currentEdge + "'. Please use 'x~y' where x and y are node ids.");
			
				System.exit(0);
			}
			
			connection = new Connection(physicalGraph.getVertexByID(Integer.parseInt(currentEdgeSplit[0])), physicalGraph.getVertexByID(Integer.parseInt(currentEdgeSplit[1])), currentEdgeRateCap, currentEdgeDelay);
			
			connections.add(connection);
			
			physicalGraph.addEdge(connection);
		}
		
		physicalGraph.setLabels();
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println("...Generation of Physical Graph finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");
		
		return physicalGraph;
	}
}