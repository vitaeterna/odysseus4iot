package odysseus4iot.deployment.store.model;

import java.util.List;

/**
 * POJO representing a Global Query. Global queries consist of several partial queries which are supposed to be distributed.
 * 
 * @author Michael Sünkel
 */
public class GlobalQuery
{
	private String name = null;
	private List<PartialQuery> partialQueries = null;
	
	private Boolean deployed = null;
	
	public GlobalQuery()
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
	
	public List<PartialQuery> getPartialQueries()
	{
		return this.partialQueries;
	}
	
	public void setPartialQueries(List<PartialQuery> partialQueries)
	{
		this.partialQueries = partialQueries;
	}
	
	public boolean isDeployed()
	{
		if(this.deployed == null)
		{
			this.deployed = Boolean.FALSE;
		}
		
		return this.deployed.booleanValue();
	}
	
	public void setDeployed(boolean deployed)
	{
		this.deployed = deployed;
	}
}