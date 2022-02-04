package odysseus4iot.graph.physical.meta;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Vertex;

/**
 * POJO representing the vertex of a physical graph.
 * 
 * @author Michael Sünkel
 */
public class Node extends Vertex
{
	public enum Type
	{
		EDGE, FOG, CLOUD
	}
	
	public String name = null;
	public String socket = null;
	public Type type = null;
	public Long cpuCapacity = null;
	public Long memCapacity = null;
	public Long cpuConsumed = null;
	public Long memConsumed = null;
	public List<Integer> ports = null;
	
	public Node(String name, String socket, Type type, Long cpuCapacity, Long memCapacity)
	{
		this.name = name;
		this.socket = socket;
		this.type = type;
		this.cpuCapacity = cpuCapacity;
		this.memCapacity = memCapacity;
		this.cpuConsumed = 0L;
		this.memConsumed = 0L;
		this.ports = new ArrayList<>();
	}
	
	@Override
	public void setLabel()
	{
		this.label = String.format("%d_%s\n%s\nCPU=%d/%d | MEM=%d/%d", this.id ,this.name ,this.type ,this.cpuConsumed ,this.cpuCapacity ,this.memConsumed ,this.memCapacity);
	}
}