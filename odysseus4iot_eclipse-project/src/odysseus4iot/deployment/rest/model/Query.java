package odysseus4iot.deployment.rest.model;

import java.util.Objects;

public class Query
{
	private Integer id       = null;
	private String name      = null;
	private String parser    = null;
	private String queryText = null;
	private String state     = null;
	private String user      = null;
	//private List<Operator> rootOperators = null;
	
	public Query()
	{
		
	}
	
	public Query(String parser, String queryText)
	{
		this.setParser(parser);
		this.setQueryText(queryText);
	}
	
	public Integer getId()
	{
		return this.id;
	}
	
	public void setId(Integer id)
	{
		this.id = id;
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
	
	public String getState()
	{
		return this.state;
	}
	
	public void setState(String state)
	{
		this.state = state;
	}
	
	public String getUser()
	{
		return this.user;
	}
	
	public void setUser(String user)
	{
		this.user = user;
	}
	
	@Override
	public boolean equals(Object object)
	{
		if(this == object)
		{
			return true;
		}
		
		if(object == null || this.getClass() != object.getClass())
		{
			return false;
		}
		
		Query query = (Query) object;
		
		return Objects.equals(this.id, query.id);
	}
	
	@Override
	public int hashCode()
	{
		return Objects.hash(this.id);
	}
}