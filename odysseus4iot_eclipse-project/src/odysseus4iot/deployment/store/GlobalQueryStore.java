package odysseus4iot.deployment.store;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import odysseus4iot.deployment.GlobalQueryScript;
import odysseus4iot.deployment.store.model.GlobalQuery;
import odysseus4iot.deployment.store.model.PartialQuery;
import odysseus4iot.deployment.store.model.Server;

//TODO: medium - persist global query store!
/**
 * The {@code GlobalQueryStore} manages global queries by name using a {@code Map}. The validity of global queries, partial queries and server information is checked.
 * 
 * @author Michael Sünkel
 */
public class GlobalQueryStore
{
	private static Map<String, GlobalQuery> globalQueries = new HashMap<String, GlobalQuery>();
	
	/**
	 * Loads a given global query into the query store.
	 * 
	 * @param globalQuery
	 */
	public static void loadGlobalQuery(GlobalQuery globalQuery)
	{
		if(globalQueryIsValid(globalQuery))
		{
			if(getGlobalQueryByName(globalQuery.getName()) == null)
			{
				globalQueries.put(globalQuery.getName(), globalQuery);
				
				GlobalQueryScript.printlnSynced("Successfully loaded global query '" + globalQuery.getName() + "' containing " + globalQuery.getPartialQueries().size() + " partial quer" + (globalQuery.getPartialQueries().size()==1?"y.":"ies."), System.out);
			}
			else
			{
				GlobalQueryScript.printlnSynced("Failed to load global query '" + globalQuery.getName() + "'. Already exists.", System.err);
			}
		}
		else
		{
			GlobalQueryScript.printlnSynced("Failed to load global query '" + globalQuery.getName() + "'. Required fields are not set properly.", System.err);
		}
	}
	
	/**
	 * Unloads a global query from the query store by a given global query name.
	 * 
	 * @param globalQuery
	 */
	public static void unloadGlobalQuery(String globalQueryName)
	{
		if(getGlobalQueryByName(globalQueryName) != null)
		{
			globalQueries.remove(globalQueryName);
			
			GlobalQueryScript.printlnSynced("Successfully unloaded global query '" + globalQueryName + "'.", System.out);
		}
		else
		{
			GlobalQueryScript.printlnSynced("Failed to unload global query '" + globalQueryName + "'. Does not exists.", System.out);
		}
	}
	
	/**
	 * Prints all global queries currently in the query store.
	 */
	public static void printAllGlobalQueries()
	{
		GlobalQuery globalQuery = null;
		
		GlobalQueryScript.printlnSynced("Name\t\t\tPartial Queries\t\t\tDeployed", System.out);
		
		for(Entry<String, GlobalQuery> globalQueryEntry : globalQueries.entrySet())
		{
			globalQuery = globalQueryEntry.getValue();
			
			GlobalQueryScript.printlnSynced(globalQuery.getName() + "\t\t\t" + globalQuery.getPartialQueries().size() + "\t\t\t" + globalQuery.isDeployed(), System.out);
		}
	}
	
	/**
	 * Retrieves a global query from the query store by the given global query namne.
	 * 
	 * @param globalQueryName
	 * @return
	 */
	public static GlobalQuery getGlobalQueryByName(String globalQueryName)
	{
		return globalQueries.get(globalQueryName);
	}
	
	/**
	 * Checks whether the given global query and contained partial queries are valid.
	 * 
	 * @param globalQuery
	 * @return
	 */
	private static boolean globalQueryIsValid(GlobalQuery globalQuery)
	{
		if(globalQuery.getName() != null)
		{
			if(!globalQuery.getName().isEmpty())
			{
				if(!globalQuery.getName().matches("[a-zA-Z][a-zA-Z0-9_-]*[a-zA-Z0-9]"))
				{
					GlobalQueryScript.printlnSynced("The name of the global query '" + globalQuery.getName() + "' is not valid. RegEx:[a-zA-Z][a-zA-Z0-9_-]*[a-zA-Z0-9]", System.err);
					
					return false;
				}
			}
			else
			{
				GlobalQueryScript.printlnSynced("The name of the global query is empty.", System.err);
				
				return false;
			}
		}
		else
		{
			GlobalQueryScript.printlnSynced("The name of the global query is null.", System.err);
			
			return false;
		}
		
		if(globalQuery.getPartialQueries() != null)
		{
			if(!globalQuery.getPartialQueries().isEmpty())
			{
				for(int index = 0; index < globalQuery.getPartialQueries().size(); index++)
				{
					if(!partialQueryIsValid(globalQuery.getPartialQueries().get(index)))
					{
						GlobalQueryScript.printlnSynced("Partial query " + (index+1) + " is not valid.", System.err);
						
						return false;
					}
				}
			}
			else
			{
				GlobalQueryScript.printlnSynced("The partial queries are empty.", System.err);
				
				return false;
			}
		}
		else
		{
			GlobalQueryScript.printlnSynced("The partial queries are null.", System.err);
			
			return false;
		}
		
		return true;
	}
	
	/**
	 * Checks whether the given partial query is valid.
	 * 
	 * @param partialQuery
	 * @return
	 */
	private static boolean partialQueryIsValid(PartialQuery partialQuery)
	{
		if(partialQuery.getName() != null)
		{
			if(!partialQuery.getName().isEmpty())
			{
				if(!partialQuery.getName().matches("[a-zA-Z][a-zA-Z0-9_-]*[a-zA-Z0-9]"))
				{
					GlobalQueryScript.printlnSynced("The name of a partial query '" + partialQuery.getName() + "' is not valid. RegEx:[a-zA-Z][a-zA-Z0-9_-]*[a-zA-Z0-9]", System.err);
					
					return false;
				}
			}
			else
			{
				GlobalQueryScript.printlnSynced("The name of a partial query is empty.", System.err);
				
				return false;
			}
		}
		else
		{
			GlobalQueryScript.printlnSynced("The name of a partial query is null.", System.err);
			
			return false;
		}
		
		if(partialQuery.getParser() != null)
		{
			if(partialQuery.getParser().isEmpty())
			{
				GlobalQueryScript.printlnSynced("The parser of a partial query is empty.", System.err);
				
				return false;
			}
		}
		else
		{
			GlobalQueryScript.printlnSynced("The parser of a partial query is null.", System.err);
			
			return false;
		}
		
		if(partialQuery.getQueryText() != null)
		{
			if(partialQuery.getQueryText().isEmpty())
			{
				GlobalQueryScript.printlnSynced("The queryText of a partial query is empty.", System.err);
				
				return false;
			}
		}
		else
		{
			GlobalQueryScript.printlnSynced("The queryText of a partial query is null.", System.err);
			
			return false;
		}
		
		if(partialQuery.getServer() != null)
		{
			if(!serverIsValid(partialQuery.getServer()))
			{
				GlobalQueryScript.printlnSynced("The server of a partial query is not valid.", System.err);
				
				return false;
			}
		}
		else
		{
			GlobalQueryScript.printlnSynced("The server of a partial query is null.", System.err);
			
			return false;
		}
		
		return true;
	}
	
	/**
	 * Checks whether the given server information is valid.
	 * 
	 * @param server
	 * @return
	 */
	private static boolean serverIsValid(Server server)
	{
		if(server.getSocket() != null)
		{
			if(server.getSocket().isEmpty())
			{
				GlobalQueryScript.printlnSynced("The socket of a server is empty.", System.err);
				
				return false;
			}
		}
		else
		{
			GlobalQueryScript.printlnSynced("The socket of a server is null.", System.err);
			
			return false;
		}
		
		if(server.getUsername() != null)
		{
			if(server.getUsername().isEmpty())
			{
				GlobalQueryScript.printlnSynced("The username of a server is empty.", System.err);
				
				return false;
			}
		}
		else
		{
			GlobalQueryScript.printlnSynced("The username of a server is null.", System.err);
			
			return false;
		}
		
		if(server.getPassword() != null)
		{
			if(server.getPassword().isEmpty())
			{
				GlobalQueryScript.printlnSynced("The password of a server is empty.", System.err);
				
				return false;
			}
		}
		else
		{
			GlobalQueryScript.printlnSynced("The password of a server is null.", System.err);
			
			return false;
		}
		
		return true;
	}
}