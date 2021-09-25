package odysseus4iot.graph.operator.meta;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Vertex;
import odysseus4iot.model.Model;

public class Operator extends Vertex
{
	public enum Type
	{
		SOURCE, SINK, MERGE, PROCESSING, BENCHMARK
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
	public Operator assignedOperator0 = null;
	public Operator assignedOperator1 = null;
	
	public Long cpuConsumption = null;
	public Long memConsumption = null;
	
	public Operator()
	{
		super();
		
		models = new ArrayList<>();
		
		cpuConsumption = 1L;
		memConsumption = 0L;
	}
	
	public Operator(Operator operator)
	{
		super(operator.id, operator.group, operator.label);
		
		this.type = operator.type;
		
		this.models = operator.models;
		
		this.inputSchema = operator.inputSchema;
		this.inputRate = operator.inputRate;
		this.inputName = operator.inputName;
		
		this.outputSchema = operator.outputSchema;
		this.outputRate = operator.outputRate;
		this.outputName = operator.outputName;
		
		this.assignedID = operator.assignedID;
		this.assignedOperator0 = operator.assignedOperator0;
		this.assignedOperator1 = operator.assignedOperator1;
		
		this.cpuConsumption = operator.cpuConsumption;
		this.memConsumption = operator.memConsumption;
	}
	
	public Operator copy()
	{
		Operator operator = new Operator();
		
		operator.id = this.id.intValue();
		operator.group = this.group.intValue();
		operator.label = this.label;
		
		operator.type = Type.valueOf(this.type.toString());
		
		operator.models = new ArrayList<>(this.models); //References to list elements are kept
		
		operator.inputSchema = this.inputSchema.copy();
		operator.inputRate = this.inputRate.doubleValue();
		operator.inputName = this.inputName;;
		
		operator.outputSchema = this.outputSchema.copy();
		operator.outputRate = this.outputRate.doubleValue();
		operator.outputName = this.outputName;
		
		operator.assignedID = this.assignedID.intValue();
		operator.assignedOperator0 = this.assignedOperator0; //Referce is kept
		operator.assignedOperator1 = this.assignedOperator1; //Referce is kept
		
		operator.cpuConsumption = this.cpuConsumption.longValue();
		operator.memConsumption = this.memConsumption.longValue();
		
		return this;
	}
	
	@Override
	public void setLabel()
	{
		this.label = String.format("%s%d_%s%s", this.inputName!=null?this.inputName+"\n":"", this.id , this.getClass().getSimpleName(), this.outputName!=null?"\n"+this.outputName:"");
	}
}