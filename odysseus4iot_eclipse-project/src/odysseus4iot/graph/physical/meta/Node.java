package odysseus4iot.graph.physical.meta;

import odysseus4iot.graph.Vertex;

public class Node extends Vertex
{
	public enum Type
	{
		EDGE, FOG, CLOUD
	}
	
	public Type type = null;
	
	public Integer cpuCapacity = null;
	public Integer memCapacity = null;
	
	public Node(Type type, Integer cpuCapacity, Integer memCapacity)
	{
		this.type = type;
		this.cpuCapacity = cpuCapacity;
		this.memCapacity = memCapacity;
	}
}