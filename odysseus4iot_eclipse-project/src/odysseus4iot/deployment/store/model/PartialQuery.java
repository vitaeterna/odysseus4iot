package odysseus4iot.deployment.store.model;

import java.util.List;

import odysseus4iot.deployment.rest.model.Query;

/**
 * POJO representing a Partial Query. Partial queries are sub queries of global queries.
 * 
 * @author Michael Sünkel
 */
public class PartialQuery
{
	private String name         = null;
	private String parser       = null;
	private String queryText    = null;
	private Server server       = null;
	
	private List<Query> queries = null;
	
	public PartialQuery()
	{
		
	}
	
	public String getName()
	{
		return this.name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
	
	public String getParser()
	{
		return this.parser;
	}
	
	public void setParser(String parser)
	{
		this.parser = parser;
	}
	
	public String getQueryText()
	{
		return this.queryText;
	}
	
	public void setQueryText(String queryText)
	{
		this.queryText = queryText;
	}
	
	public Server getServer()
	{
		return this.server;
	}
	
	public void setServer(Server server)
	{
		this.server = server;
	}
	
	public List<Query> getQueries()
	{
		return this.queries;
	}
	
	public void setQueries(List<Query> queries)
	{
		this.queries = queries;
	}
	
	public void removeQuery(Query query)
	{
		if(this.getQueries() != null)
		{
			this.getQueries().remove(query);
			
			if(this.getQueries().isEmpty())
			{
				this.setQueries(null);
			}
		}
	}
	
	public Query getQuery()
	{
		Query query = new Query();
		
		query.setParser(this.getParser());
		query.setQueryText(this.getQueryText());
		
		return query;
	}
}