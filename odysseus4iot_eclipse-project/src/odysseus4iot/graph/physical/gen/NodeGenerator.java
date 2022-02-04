package odysseus4iot.graph.physical.gen;

import odysseus4iot.graph.physical.meta.Node;
import odysseus4iot.graph.physical.meta.Node.Type;

/**
 * The {@code NodeGenerator} provides methods to generate {@link Node} objects.
 * 
 * @author Michael Sünkel
 */
public class NodeGenerator
{
	public static Node generateNode(String name, String socket, Type type, Long cpuCapacity, Long memCapacity)
	{
		Node node = new Node(name, socket, type, cpuCapacity, memCapacity);
		
		return node;
	}
}