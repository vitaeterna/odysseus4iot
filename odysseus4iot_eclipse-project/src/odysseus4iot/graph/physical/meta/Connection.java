package odysseus4iot.graph.physical.meta;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Vertex;

public class Connection extends Edge
{
	public Integer datarateCapacity = null;
	
	public Connection(Vertex vertex0, Vertex vertex1)
	{
		super(vertex0, vertex1);
	}
	
	public Connection(Vertex vertex0, Vertex vertex1, Integer datarateCapacity)
	{
		super(vertex0, vertex1);
		
		this.datarateCapacity = datarateCapacity;
	}
}