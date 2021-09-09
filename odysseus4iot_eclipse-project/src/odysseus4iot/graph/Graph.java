package odysseus4iot.graph;

import java.util.ArrayList;
import java.util.List;

public class Graph
{
	public List<Vertex> vertices = null;
	public List<Edge> edges = null;
	
	public void addVertex(Vertex vertex)
	{
		if(this.vertices == null)
		{
			this.vertices = new ArrayList<>();
		}
		
		if(!this.vertices.contains(vertex))
		{
			this.vertices.add(vertex);
		}
		else
		{
			System.err.println("Graph already contains this Vertex!");
		}
	}
	
	public void addEdge(Edge edge)
	{
		if(this.edges == null)
		{
			this.edges = new ArrayList<>();
		}
		
		if(!this.vertices.contains(edge.vertex0) || !this.vertices.contains(edge.vertex1))
		{
			if(!this.edges.contains(edge))
			{
				if(!this.containsEdge(edge))
				{
					this.edges.add(edge);
				}
				else
				{
					System.err.println("Graph already contains an edge between those vertices!");
				}
			}
			else
			{
				System.err.println("Graph already contains this edge!");
			}
		}
		else
		{
			System.err.println("All vertices must be added to the Graph before adding this edge!");
		}
	}
	
	public List<Vertex> getVertexSuccessors(Vertex vertex)
	{
		List<Vertex> successors = new ArrayList<>();
		
		if(this.vertices == null || this.edges == null)
		{
			return successors;
		}
		
		Edge currentEdge = null;
		
		for(int index = 0; index < this.edges.size(); index++)
		{
			currentEdge = this.edges.get(index);
			
			if(currentEdge.vertex0 == vertex)
			{
				successors.add(currentEdge.vertex1);
			}
		}
		
		return successors;
	}
	
	public List<Vertex> getPredecessors(Vertex vertex)
	{
		List<Vertex> predecessors = new ArrayList<>();
		
		if(this.vertices == null || this.edges == null)
		{
			return predecessors;
		}
		
		Edge currentEdge = null;
		
		for(int index = 0; index < this.edges.size(); index++)
		{
			currentEdge = this.edges.get(index);
			
			if(currentEdge.vertex1 == vertex)
			{
				predecessors.add(currentEdge.vertex0);
			}
		}
		
		return predecessors;
	}
	
	private boolean containsEdge(Edge edge)
	{
		if(this.edges == null)
		{
			return false;
		}
		
		Edge currentEdge = null;
		
		for(int index = 0; index < this.edges.size(); index++)
		{
			currentEdge = this.edges.get(index);
			
			if(currentEdge.vertex0 == edge.vertex0 && currentEdge.vertex1 == edge.vertex1)
			{
				return true;
			}
		}
		
		return false;
	}
}