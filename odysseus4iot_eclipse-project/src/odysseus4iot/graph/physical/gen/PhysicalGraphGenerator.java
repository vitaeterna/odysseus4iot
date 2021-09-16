package odysseus4iot.graph.physical.gen;

import java.util.List;

import odysseus4iot.graph.Graph;

public class PhysicalGraphGenerator
{
	public static Graph generatePhysicalraph(List<String> nodes, List<String> nodeSockets, List<String> nodeTypes, List<String> nodeCPUCaps, List<String> nodeMemCaps, List<String> edges, List<String> edgeRateCaps)
	{
		//TODO
		
		String currentNode = null;
		String currentNodeSocket = null;
		String currentNodeType = null;
		String currentNodeCPUCap = null;
		String currentNodeMemCap = null;
		
		for(int index = 0; index < nodes.size(); index++)
		{
			currentNode = nodes.get(index);
			currentNodeSocket = nodeSockets.get(index);
			currentNodeType = nodeTypes.get(index);
			currentNodeCPUCap = nodeCPUCaps.get(index);
			currentNodeMemCap = nodeMemCaps.get(index);
			
			
		}
		
		String currentEdge = null;
		String currentEdgeRateCap = null;
		
		for(int index = 0; index < edges.size(); index++)
		{
			currentEdge = edges.get(index);
			currentEdgeRateCap = edgeRateCaps.get(index);
		}
		
		return null;
	}
}