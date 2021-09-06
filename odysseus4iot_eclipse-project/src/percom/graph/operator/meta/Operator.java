package percom.graph.operator.meta;

import percom.graph.Vertex;

public class Operator extends Vertex
{
	public Schema inputSchema = null;
	public Double inputRate = null;
	public String inputName = null;
	
	public Schema outputSchema = null;
	public Double outputRate = null;
	public String outputName = null;
}