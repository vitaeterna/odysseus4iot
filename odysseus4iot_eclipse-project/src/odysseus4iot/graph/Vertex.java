package odysseus4iot.graph;

public abstract class Vertex
{
	public Integer id = null;
	public Integer group = null;
	public String label = null;
	
	public Vertex()
	{
		
	}
	
	public Vertex(Integer id, Integer group, String label)
	{
		this.id = id;
		this.group = group;
		this.label = label;
	}
	
	public void setLabel()
	{
		this.label = String.format("%d_%d", this.id ,this.group);
	}
}