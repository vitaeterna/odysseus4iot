package odysseus4iot.graph.physical.meta;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Vertex;
import odysseus4iot.util.Util;

public class Connection extends Edge
{
	public Integer datarateCapacity = null;
	public Double datarateConsumed = null;
	
	public Connection(Vertex vertex0, Vertex vertex1)
	{
		super(vertex0, vertex1);
	}
	
	public Connection(Vertex vertex0, Vertex vertex1, Integer datarateCapacity)
	{
		super(vertex0, vertex1);
		
		this.datarateCapacity = datarateCapacity;
		
		this.datarateConsumed = 0.0d;
	}
	
	@Override
	public void setLabel()
	{
		this.label = String.format("%s/%s", Util.formatSizeInBits(datarateConsumed), Util.formatSizeInBits((double)this.datarateCapacity));
	}
}