package odysseus4iot.graph.operator.meta;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Vertex;

public class DataFlow extends Edge
{
	public Double datarateConsumption = null;
	
	public DataFlow(Vertex vertex0, Vertex vertex1)
	{
		super(vertex0, vertex1);
	}
	
	public DataFlow(Vertex vertex0, Vertex vertex1, Double datarateConsumption)
	{
		super(vertex0, vertex1);
		
		this.datarateConsumption = datarateConsumption;
	}
}