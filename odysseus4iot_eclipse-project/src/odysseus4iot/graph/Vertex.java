package odysseus4iot.graph;

public abstract class Vertex
{
	public Integer id = null;
	public Integer group = null;
	public String label = null;
	
	public void setLabel()
	{
		this.label = String.format("%d_%d", this.id ,this.group);
	}
}