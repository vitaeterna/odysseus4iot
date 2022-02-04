package odysseus4iot.graph.operator.meta;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Vertex;
import odysseus4iot.util.Util;

/**
 * POJO representing the edge of an operator graph.
 * 
 * @author Michael Sünkel
 */
public class DataFlow extends Edge
{
	public Double datarateConsumption = null;
	
	public DataFlow(Vertex vertex0, Vertex vertex1)
	{
		super(vertex0, vertex1);
	}
	
	public DataFlow copy()
	{
		DataFlow dataFlow = new DataFlow(this.vertex0, this.vertex1);
		
		dataFlow.label = this.label;
		
		dataFlow.datarateConsumption = this.datarateConsumption.doubleValue();
		
		return dataFlow;
	}
	
	@Override
	public void setLabel()
	{
		Schema outputSchema = ((Operator)this.vertex0).outputSchema;
		
		this.label = String.format("%s\n%s/s", outputSchema.columns.size()>10?outputSchema.columns.size():outputSchema.toString(), Util.formatSizeInBits(datarateConsumption));
	}
}