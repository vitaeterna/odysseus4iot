package odysseus4iot.graph.physical.meta;

import odysseus4iot.graph.Edge;

public class Connection extends Edge
{
	public Integer maxDatarate = null;
	
	public Connection(Integer maxDatarate)
	{
		this.maxDatarate = maxDatarate;
	}
}