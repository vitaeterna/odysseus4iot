package odysseus4iot.graph;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.operator.MergeOperator;
import odysseus4iot.graph.operator.ProjectOperator;

public class Graph
{
	public List<Vertex> vertices = null;
	public List<Edge> edges = null;
	
	public Graph()
	{
		Vertex.resetIDs();
		MergeOperator.resetMergeCount();
		ProjectOperator.resetProjectCount();
	}
	
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
			System.err.println("Graph already contains vertex " + vertex.id + "!");
		}
	}
	
	public void addEdge(Edge edge)
	{
		if(this.edges == null)
		{
			this.edges = new ArrayList<>();
		}
		
		if(this.vertices.contains(edge.vertex0) && this.vertices.contains(edge.vertex1))
		{
			if(!this.edges.contains(edge))
			{
				if(!this.containsEdge(edge))
				{
					this.edges.add(edge);
				}
				else
				{
					System.err.println("Graph already contains an edge between vertices " + edge.vertex0.id + " -> " + edge.vertex1.id + "!");
				}
			}
			else
			{
				System.err.println("Graph already contains this edge " + edge.vertex0.id + " -> " + edge.vertex1.id + "!");
			}
		}
		else
		{
			System.err.println("All vertices must be added to the Graph before adding the edge " + edge.vertex0.id + " -> " + edge.vertex1.id + "!");
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
	
	public List<Vertex> getVerticesByType(Class<? extends Vertex> type)
	{
		List<Vertex> requestedVertices = new ArrayList<>();
		
		Vertex currentVertex = null;
		
		for(int index = 0; index < this.vertices.size(); index++)
		{
			currentVertex = this.vertices.get(index);
			
			if(currentVertex.getClass() == type)
			{
				requestedVertices.add(currentVertex);
			}
		}
		
		return requestedVertices;
	}
	
	public Vertex getVertexByID(int id)
	{
		Vertex currentVertex = null;
		
		for(int index = 0; index < this.vertices.size(); index++)
		{
			currentVertex = this.vertices.get(index);
			
			if(currentVertex.id.intValue() == id)
			{
				return currentVertex;
			}
		}
		
		return null;
	}
	
	public List<Vertex> getStartingVertices()
	{
		List<Vertex> startingVertices = new ArrayList<>();
		
		Vertex currentVertex = null;
		Edge currentEdge = null;
		
		boolean isStartingVertex = true;
		
		for(int index = 0; index < this.vertices.size(); index++)
		{
			currentVertex = this.vertices.get(index);
			
			isStartingVertex = true;
			
			for(int index2 = 0; index2 < this.edges.size(); index2++)
			{
				currentEdge = this.edges.get(index2);
				
				if(currentVertex == currentEdge.vertex1)
				{
					isStartingVertex = false;
				}
			}
			
			if(isStartingVertex)
			{
				startingVertices.add(currentVertex);
			}
		}
		
		return startingVertices;
	}
	
	public List<Vertex> getVerticesBreadthFirst()
	{
		List<Vertex> vertices = this.getStartingVertices();
		List<Vertex> successors = null;
		
		Vertex currentVertex = null;
		Vertex currentSuccessor = null;
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentVertex = vertices.get(index);
			
			successors = this.getVertexSuccessors(currentVertex);
			
			for(int index2 = 0; index2 < successors.size(); index2++)
			{
				currentSuccessor = successors.get(index2);
				
				if(!vertices.contains(currentSuccessor))
				{
					vertices.add(currentSuccessor);
				}
			}
		}
		
		return vertices;
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