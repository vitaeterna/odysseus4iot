package odysseus4iot.graph.operator.meta;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Vertex;
import odysseus4iot.model.Model;

public class Operator extends Vertex
{
	public enum Type
	{
		SOURCE, SINK, MERGE, PROCESSING
	}
	
	public Type type = null;
	
	public List<Model> models = null;
	
	public Schema inputSchema = null;
	public Double inputRate = null;
	public String inputName = null;
	
	public Schema outputSchema = null;
	public Double outputRate = null;
	public String outputName = null;
	
	public Integer assignedID = null;
	
	public Integer cpuConsumption = null;
	public Integer memConsumption = null;
	
	public Operator()
	{
		super();
		
		models = new ArrayList<>();
		
		//TODO: set consumptions
		cpuConsumption = 0;
		memConsumption = 0;
	}
	
	@Override
	public void setLabel()
	{
		this.label = String.format("%s%d_%s%s", this.inputName!=null?this.inputName+"\n":"", this.id , this.getClass().getSimpleName(), this.outputName!=null?"\n"+this.outputName:"");
	}
}