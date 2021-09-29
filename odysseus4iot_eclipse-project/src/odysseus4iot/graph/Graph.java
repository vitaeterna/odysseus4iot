package odysseus4iot.graph;

import java.util.ArrayList;
import java.util.List;

public abstract class Graph
{
	private Integer currentVertexID = null;
	private Integer currentVertexGroup = null;
	
	public Integer getNextVertexID()
	{
		return ++currentVertexID;
	}
	
	public Integer getCurrentGroup()
	{
		return currentVertexGroup;
	}
	
	public Integer getNextGroup()
	{
		return currentVertexGroup += 3;
	}
	
	public String label = null;
	public List<Vertex> vertices = null;
	public List<Edge> edges = null;
	
	public Graph(String label)
	{
		this.label = label;
		
		this.vertices = new ArrayList<>();
		this.edges = new ArrayList<>();
		
		currentVertexID = 0;
		currentVertexGroup = 0;
	}
	
	public void addVertex(Vertex vertex)
	{
		if(vertex == null)
		{
			return;
		}
		
		if(!this.vertices.contains(vertex))
		{
			vertex.id = this.getNextVertexID();
			vertex.group = this.getCurrentGroup();
			
			this.vertices.add(vertex);
		}
		else
		{
			System.err.println("Graph already contains vertex " + vertex.id + "!");
		}
	}
	
	public void addAllVertices(List<Vertex> vertices)
	{
		for(int index = 0; index < vertices.size(); index++)
		{
			this.addVertex(vertices.get(index));
		}
	}
	
	public void addEdge(Edge edge)
	{
		if(edge == null)
		{
			return;
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
	
	public void addAllEdges(List<Edge> edges)
	{
		for(int index = 0; index < edges.size(); index++)
		{
			this.addEdge(edges.get(index));
		}
	}
	
	public List<Vertex> getSuccessors(Vertex vertex)
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
	
	public int getMinVertexID()
	{
		Vertex currentVertex = null;
		
		int minVertexID = Integer.MAX_VALUE;
		
		for(int index = 0; index < this.vertices.size(); index++)
		{
			currentVertex = this.vertices.get(index);
			
			if(currentVertex.id.intValue() < minVertexID)
			{
				minVertexID = currentVertex.id.intValue();
			}
		}
		
		return minVertexID;
	}
	
	public int getMaxVertexID()
	{
		Vertex currentVertex = null;
		
		int maxVertexID = Integer.MIN_VALUE;
		
		for(int index = 0; index < this.vertices.size(); index++)
		{
			currentVertex = this.vertices.get(index);
			
			if(currentVertex.id.intValue() > maxVertexID)
			{
				maxVertexID = currentVertex.id.intValue();
			}
		}
		
		return maxVertexID;
	}
	
	public List<Vertex> getStartingVertices()
	{
		if(this.vertices == null)
		{
			
		}
		
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
	
	public List<Vertex> getVerticesBreadthFirst(Vertex startingVertex)
	{
		List<Vertex> vertices = null;
		List<Vertex> successors = null;
		
		Vertex currentVertex = null;
		Vertex currentSuccessor = null;
		
		if(startingVertex == null)
		{
			vertices = this.getStartingVertices();
		}
		else
		{
			vertices = new ArrayList<>();
			vertices.add(startingVertex);
		}
		
		for(int index = 0; index < vertices.size(); index++)
		{
			currentVertex = vertices.get(index);
			
			successors = this.getSuccessors(currentVertex);
			
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
	
	public List<Edge> getInputEdges(Vertex vertex)
	{
		List<Edge> inputEdges = new ArrayList<>();
		
		Edge currentEdge = null;
		
		for(int index = 0; index < this.edges.size(); index++)
		{
			currentEdge = edges.get(index);
			
			if(currentEdge.vertex1 == vertex)
			{
				inputEdges.add(currentEdge);
			}
		}
		
		return inputEdges;
	}
	
	public List<Edge> getOutputEdges(Vertex vertex)
	{
		List<Edge> outputEdges = new ArrayList<>();
		
		Edge currentEdge = null;
		
		for(int index = 0; index < this.edges.size(); index++)
		{
			currentEdge = edges.get(index);
			
			if(currentEdge.vertex0 == vertex)
			{
				outputEdges.add(currentEdge);
			}
		}
		
		return outputEdges;
	}
	
	public Edge getEdge(Vertex vertex0, Vertex vertex1)
	{
		Edge currentEdge = null;
		
		for(int index = 0; index < this.edges.size(); index++)
		{
			currentEdge = edges.get(index);
			
			if(currentEdge.vertex0 == vertex0 && currentEdge.vertex1 == vertex1)
			{
				return currentEdge;
			}
		}
		
		return null;
	}
	
	public void setLabels()
	{
		for(int index = 0; index < this.vertices.size(); index++)
		{
			this.vertices.get(index).setLabel();
		}
		
		for(int index = 0; index < this.edges.size(); index++)
		{
			this.edges.get(index).setLabel();
		}
	}
	
	public boolean isEmpty()
	{
		return this.vertices.isEmpty() && this.edges.isEmpty();
	}
	
	public boolean containsEdge(int id0, int id1)
	{
		Edge currentEdge = null;
		
		for(int index = 0; index < this.edges.size(); index++)
		{
			currentEdge = this.edges.get(index);
			
			if(currentEdge.vertex0.id == id0 && currentEdge.vertex1.id == id1)
			{
				return true;
			}
		}
		
		return false;
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