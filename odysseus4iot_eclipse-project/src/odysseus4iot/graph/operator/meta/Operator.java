package odysseus4iot.graph.operator.meta;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Vertex;
import odysseus4iot.model.Model;

public class Operator extends Vertex
{
	public List<Model> models = null;
	
	public Schema inputSchema = null;
	public Double inputRate = null;
	public String inputName = null;
	
	public Schema outputSchema = null;
	public Double outputRate = null;
	public String outputName = null;
	
	public Operator()
	{
		super();
		
		models = new ArrayList<>();
	}
}