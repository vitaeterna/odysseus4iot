package odysseus4iot.graph.physical.meta;

import odysseus4iot.graph.Vertex;

public class Node extends Vertex
{
	public enum Type
	{
		EDGE, FOG, CLOUD
	}
	
	public String name = null;
	public String socket = null;
	public Type type = null;
	public Integer cpuCapacity = null;
	public Integer memCapacity = null;
	public Double cpuConsumed = null;
	public Double memConsumed = null;
	
	public Node(String name, String socket, Type type, Integer cpuCapacity, Integer memCapacity)
	{
		this.name = name;
		this.socket = socket;
		this.type = type;
		this.cpuCapacity = cpuCapacity;
		this.memCapacity = memCapacity;
		this.cpuConsumed = 0.0d;
		this.memConsumed = 0.0d;
	}
	
	@Override
	public void setLabel()
	{
		this.label = String.format("%d_%s\n%s\nCPU=%.1f/%d | MEM=%.1f/%d", this.id ,this.name ,this.type ,this.cpuConsumed ,this.cpuCapacity ,this.memConsumed ,this.memCapacity);
	}
}