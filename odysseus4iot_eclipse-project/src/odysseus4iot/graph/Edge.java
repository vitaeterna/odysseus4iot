package odysseus4iot.graph;

public abstract class Edge
{
	public Vertex vertex0 = null;
	public Vertex vertex1 = null;
	public String label = null;
	
	public Edge()
	{
		
	}
	
	public Edge(Vertex vertex0, Vertex vertex1)
	{
		this.vertex0 = vertex0;
		this.vertex1 = vertex1;
	}
	
	public void setLabel()
	{
		this.label = String.format("%d->%d", this.vertex0.id, this.vertex1.id);
	}
}