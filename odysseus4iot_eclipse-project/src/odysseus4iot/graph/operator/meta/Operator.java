package odysseus4iot.graph.operator.meta;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Vertex;
import odysseus4iot.model.Model;

public class Operator extends Vertex
{
	public enum Type
	{
		SOURCE, SINK, MERGE, PROJECT, PROCESSING, BENCHMARK, NOP
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
	public List<Operator> assignedOperators0 = null;
	public List<Operator> assignedOperators1 = null;
	
	public Long cpuConsumption = null;
	public Long memConsumption = null;
	
	public Operator()
	{
		super();
		
		models = new ArrayList<>();
		
		assignedID = 0;
		
		assignedOperators0 = new ArrayList<>();
		assignedOperators1 = new ArrayList<>();
		
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
		this.assignedOperators0 = new ArrayList<>(operator.assignedOperators0);
		this.assignedOperators1 = new ArrayList<>(operator.assignedOperators1);
		
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
		
		operator.inputSchema = this.inputSchema==null?null:this.inputSchema.copy();
		operator.inputRate = this.inputRate==null?null:this.inputRate.doubleValue();
		operator.inputName = this.inputName;
		
		operator.outputSchema = this.outputSchema.copy();
		operator.outputRate = this.outputRate.doubleValue();
		operator.outputName = this.outputName;
		
		operator.assignedID = this.assignedID.intValue();
		operator.assignedOperators0 = new ArrayList<>(this.assignedOperators0); //References within the list are kept
		operator.assignedOperators1 = new ArrayList<>(this.assignedOperators1); //References within the list are kept
		
		operator.cpuConsumption = this.cpuConsumption.longValue();
		operator.memConsumption = this.memConsumption.longValue();
		
		return operator;
	}
	
	@Override
	public void setLabel()
	{
		this.label = String.format("%s%d_%s%s", this.inputName!=null?this.inputName+"\n":"", this.id , this.getClass().getSimpleName(), this.outputName!=null?"\n"+this.outputName:"");
	}
}