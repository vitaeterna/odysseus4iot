package odysseus4iot.deployment;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.List;

import com.google.gson.JsonSyntaxException;

import odysseus4iot.deployment.json.OdysseusJsonClient;
import odysseus4iot.deployment.rest.OdysseusRestClient;
import odysseus4iot.deployment.rest.model.Query;
import odysseus4iot.deployment.rest.model.Token;
import odysseus4iot.deployment.rest.model.User;
import odysseus4iot.deployment.store.GlobalQueryStore;
import odysseus4iot.deployment.store.model.GlobalQuery;
import odysseus4iot.deployment.store.model.PartialQuery;
import odysseus4iot.deployment.store.model.Server;

public class GlobalQueryScript
{
	public static final String COMMAND_PROMPT   = ">";
	public static final String COMMAND_LOAD     = "load";
	public static final String COMMAND_UNLOAD   = "unload";
	public static final String COMMAND_LIST     = "list";
	public static final String COMMAND_DEPLOY   = "deploy";
	public static final String COMMAND_UNDEPLOY = "undeploy";
	public static final String COMMAND_EXIT_0   = "exit";
	public static final String COMMAND_EXIT_1   = "quit";
	
	public static void main(String[] args)
	{
		printlnSynced("Welcome to GlobalQueryScript(GQS) for Odysseus\n", System.out);
		printlnSynced("This tool can be used in two different modes:", System.out);
		printlnSynced("   interactive mode - Passing 0 arguments you can interactively enter commands one by one", System.out);
		printlnSynced("   script mode      - Passing 1 argument you can run a predefined script (.gqs)\n", System.out);
		printlnSynced("The following commands are available:", System.out);
		printlnSynced("   load <file>      - loads a global query from a json file", System.out);
		printlnSynced("   unload <qname>   - unloads a global query by query name", System.out);
		printlnSynced("   list             - lists all loaded global queries", System.out);
		printlnSynced("   deploy <qname>   - deploys a global query by query name", System.out);
		printlnSynced("   undeploy <qname> - undeploys a global query by query name", System.out);
		printlnSynced("   exit             - terminates the application", System.out);
		printlnSynced("   quit             - terminates the application\n", System.out);
		
		if(args == null || args.length == 0)
		{
			printlnSynced("You have entered the interactive mode.\n", System.out);
			
	        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
	        
	        while(true)
	        {
	        	printSynced(COMMAND_PROMPT, System.out);
		        
		        String lineOfScript = null;
		        
				try
				{
					synchronized(GlobalQueryScript.class)
					{
						lineOfScript = bufferedReader.readLine(); //TODO: minor - FIX COPY PASTE MECHANICS and async writing in console
					}
				}
				catch(IOException e)
				{
	    			printlnSynced(e.getClass().getName() + ": " + e.getMessage(), System.err);
	    			
	    			return;
				}
		        
		        executeCommand(lineOfScript);
	        }
		}
		else if(args.length == 1)
		{
			printlnSynced("You have entered the script mode.\n", System.out);
			
			args[0] = args[0].replaceAll("[\'\"]+", "");
    		
    		File file = new File(args[0]);
    		
    		Path path = null;
    		
    		try
    		{
    			path = file.toPath();
    		}
    		catch(InvalidPathException e)
    		{
    			printlnSynced(e.getClass().getName() + ": " + e.getMessage(), System.err);
    			
    			return;
    		}
    		
        	String script = null;

        	try
        	{
        		script = Files.readString(path, StandardCharsets.UTF_8);
        	}
        	catch(IOException | OutOfMemoryError | SecurityException e)
        	{
    			printlnSynced(e.getClass().getName() + ": " + e.getMessage(), System.err);
    			
    			return;
        	}
        	
        	if(args[0].endsWith(".gqs"))
        	{
            	printlnSynced("Interpreting " + args[0] + "...", System.out);
    			
            	BufferedReader bufferedReader = new BufferedReader(new StringReader(script));

            	String lineOfScript = null;
            	
            	while(true)
            	{
            		try
            		{
    					lineOfScript = bufferedReader.readLine();
    				}
            		catch(IOException e)
            		{
            			printlnSynced(e.getClass().getName() + ": " + e.getMessage(), System.err);
            			
            			return;
    				}
            		
            		if(lineOfScript != null)
            		{
            			printlnSynced(COMMAND_PROMPT + lineOfScript, System.out);
            			
            			executeCommand(lineOfScript);
            		}
            		else
            		{
            			break;
            		}
            	}
            	
            	printlnSynced("Interpreting complete.", System.out);
        	}
        	else
        	{
        		printlnSynced("The given file '" + args[0] + "' does not end with .gqs", System.err);
        	}
		}
		else
		{
			printlnSynced("You have entered too many arguments.", System.out);
		}
	}
	
	private static void executeCommand(String lineOfScript)
	{
		String[] splitCommand = lineOfScript.split("\\s+(?=(?:[^\\'\"]*[\\'\"][^\\'\"]*[\\'\"])*[^\\'\"]*$)"); //TODO: minor - REMOVE QUOTE MIXTURE
		
		if(splitCommand != null && splitCommand.length > 0)
        {
	        if(splitCommand[0].equals(""))
	        {
	        	
	        }
	        else if(splitCommand[0].equals(COMMAND_LOAD))
	        {
	        	if(splitCommand.length == 2)
	        	{
	        		splitCommand[1] = splitCommand[1].replaceAll("[\'\"]+", "");
	        		
	        		File file = new File(splitCommand[1]);
	        		
	        		Path path = null;
	        		
	        		try
	        		{
	        			path = file.toPath();
	        		}
	        		catch(InvalidPathException e)
	        		{
	        			printlnSynced(e.getClass().getName() + ": " + e.getMessage(), System.err);
	        			
	        			return;
	        		}
	        		
		        	String json = null;

		        	try
		        	{
		        		json = Files.readString(path, StandardCharsets.UTF_8);
		        	}
		        	catch(IOException | OutOfMemoryError | SecurityException e)
		        	{
	        			printlnSynced(e.getClass().getName() + ": " + e.getMessage(), System.err);
	        			
	        			return;
		        	}
		        	
		        	GlobalQuery globalQuery = null;
		        	
		        	try
		        	{
		        		globalQuery = OdysseusJsonClient.getGlobalQueryFromJson(json);
		        	}
		        	catch(JsonSyntaxException e)
		        	{
	        			printlnSynced(e.getClass().getName() + ": " + e.getMessage(), System.err);
	        			
	        			return;
		        	}
		        	
		        	GlobalQueryStore.loadGlobalQuery(globalQuery);
	        	}
	        	else
	        	{
	        		printlnSynced("Invalid number of arguments." + " [" + (splitCommand.length - 1) + "]", System.out);
	        	}
	        }
	        else if(splitCommand[0].equals(COMMAND_UNLOAD))
	        {
	        	if(splitCommand.length == 2)
	        	{
		        	GlobalQueryStore.unloadGlobalQuery(splitCommand[1]);
	        	}
	        	else
	        	{
	        		printlnSynced("Invalid number of arguments." + " [" + (splitCommand.length - 1) + "]", System.out);
	        	}
	        }
	        else if(splitCommand[0].equals(COMMAND_LIST))
	        {
	        	if(splitCommand.length == 1)
	        	{
	        		GlobalQueryStore.printAllGlobalQueries();
	        	}
	        	else
	        	{
	        		printlnSynced("Invalid number of arguments." + " [" + (splitCommand.length - 1) + "]", System.out);
	        	}
	        }
	        else if(splitCommand[0].equals(COMMAND_DEPLOY))
	        {
	        	//TODO: major - implement rollback in case of error at some point
	        	if(splitCommand.length == 2)
	        	{
	        		GlobalQuery globalQuery = GlobalQueryStore.getGlobalQueryByName(splitCommand[1]);
	        		
	        		if(globalQuery != null)
	        		{
	        			printlnSynced("Deploying of global query '" + globalQuery.getName() + "' started.", System.out);
	        			
	        			Token token = null;
	        			
	        			PartialQuery partialQuery = null;
	        			
	        			Server server = null;
	        			
	        			boolean deployed = true;
	        			
	    				for(int index = 0; index < globalQuery.getPartialQueries().size(); index++)
	    				{
	    					partialQuery = globalQuery.getPartialQueries().get(index);
	    					
	    					printlnSynced("Deploying of partial query '" + partialQuery.getName() + "' started.", System.out);
	    					
	    					server = partialQuery.getServer();
	    					
				        	token = OdysseusRestClient.post_login(server.getSocket(), new User(server.getUsername(), server.getPassword()));
				        	
				        	if(token == null)
				        	{
				        		printlnSynced("Login to " + server.getSocket() + " failed.", System.err);
				        		
				        		printlnSynced("Deploying of global query '" + globalQuery.getName() + "' failed. There is no rollback mechanic implemented so far.", System.err);

				        		deployed = false;
				        		
				        		break;
				        	}
				        	else
				        	{
				        		printlnSynced("Successfully logged in to " + server.getSocket() + ". (token:\"" + token.getToken() + "\")", System.out);
				        		
				        		deployed = OdysseusRestClient.post_query(server.getSocket(), token, partialQuery.getQuery(), partialQuery);
				        	
				        		if(deployed)
				        		{
				        			printlnSynced("Successfully deployed partial query '" + partialQuery.getName() + "'.", System.out);
				        		}
				        		else
				        		{
				        			printlnSynced("Deploying of partial query '" + partialQuery.getName() + "' failed.", System.err);
					        		
					        		printlnSynced("Deploying of global query '" + globalQuery.getName() + "' failed. There is no rollback mechanic implemented so far.", System.err);
				        		}
				        	}
	    				}
	    				
	    				if(deployed)
	    				{
		    				globalQuery.setDeployed(true);
		    				
		    				printlnSynced("Successfully deployed global query '" + globalQuery.getName() + "'.", System.out);
	    				}
	        		}
	        		else
	        		{
	        			printlnSynced("The global query '" + splitCommand[1] + "' could not be found in the global query store.", System.out);
	        		}
	        	}
	        	else
	        	{
	        		printlnSynced("Invalid number of arguments." + " [" + (splitCommand.length - 1) + "]", System.out);
	        	}
	        }
	        else if(splitCommand[0].equals(COMMAND_UNDEPLOY))
	        {
	        	if(splitCommand.length == 2)
	        	{
	        		GlobalQuery globalQuery = GlobalQueryStore.getGlobalQueryByName(splitCommand[1]);
	        		
	        		if(globalQuery != null)
	        		{
	        			if(globalQuery.isDeployed())
	        			{
		        			printlnSynced("Undeploying of global query '" + globalQuery.getName() + "' started.", System.out);
		        			
		        			Token token = null;
		        			
		        			PartialQuery partialQuery = null;
		        			
		        			Server server = null;
		        			
		        			boolean undeployed = true;
		        			
		    				for(int index = 0; index < globalQuery.getPartialQueries().size(); index++)
		    				{
		    					partialQuery = globalQuery.getPartialQueries().get(index);
		    					
		    					printlnSynced("Undeploying of partial query '" + partialQuery.getName() + "' started.", System.out);
		    					
		    					server = partialQuery.getServer();
		    					
					        	token = OdysseusRestClient.post_login(server.getSocket(), new User(server.getUsername(), server.getPassword()));
					        	
					        	if(token == null)
					        	{
					        		printlnSynced("Login to " + server.getSocket() + " failed.", System.err);
					        		
					        		printlnSynced("Deploying of global query '" + globalQuery.getName() + "' failed. There is no rollback mechanic implemented so far.", System.err);
	
					        		undeployed = false;
					        		
					        		break;
					        	}
					        	else
					        	{
					        		printlnSynced("Successfully logged in to " + server.getSocket() + ". (token:\"" + token.getToken() + "\")", System.out);
					        		
					        		List<Query> queries = partialQuery.getQueries();
					        		
					        		for(int index2 = 0; index2  < queries.size(); index2++)
					        		{
					        			if(OdysseusRestClient.delete_query(server.getSocket(), token, queries.get(index2), partialQuery))
					        			{
					        				index2--;
					        			}
					        			else
					        			{
					        				undeployed = false;
					        				
					        				printlnSynced("Undeploying of query '" + queries.get(index2).getId() + "' failed.", System.err);
					        			}
					        		}
					        		
					        		if(undeployed)
					        		{
					        			printlnSynced("Successfully undeployed partial query '" + partialQuery.getName() + "'.", System.out);
					        		}
					        		else
					        		{
					        			printlnSynced("Undeploying of partial query '" + partialQuery.getName() + "' failed.", System.err);
						        		
						        		printlnSynced("Undeploying of global query '" + globalQuery.getName() + "' failed. There is no rollback mechanic implemented so far.", System.err);
					        		
						        		break;
					        		}
					        	}
		    				}
		    				
		    				if(undeployed)
		    				{
			    				globalQuery.setDeployed(false);
			    				
			    				printlnSynced("Successfully undeployed global query '" + globalQuery.getName() + "'.", System.out);
		    				}
	        			}
	        			else
	        			{
	        				printlnSynced("The global query '" + splitCommand[1] + "' is not deployed. No need to undeploy.", System.out);
	        			}
	        		}
	        		else
	        		{
	        			printlnSynced("The global query '" + splitCommand[1] + "' could not be found in the global query store.", System.out);
	        		}
	        	}
	        	else
	        	{
	        		printlnSynced("Invalid number of arguments." + " [" + (splitCommand.length - 1) + "]", System.out);
	        	}
	        }
	        else if(splitCommand[0].equals(COMMAND_EXIT_0) || splitCommand[0].equals(COMMAND_EXIT_1))
	        {
	        	if(splitCommand.length == 1)
	        	{
		        	printlnSynced("Terminating.", System.out);
		        	
		        	System.exit(0);
	        	}
	        	else
	        	{
	        		printlnSynced("Invalid number of arguments." + " [" + (splitCommand.length - 1) + "]", System.out);
	        	}
	        }
	        else
	        {
	        	printlnSynced("Unknown command.", System.out);
	        }
        }
	}
	
	public static synchronized void printSynced(String content, PrintStream printStream)
	{
		printStream.print(content);
	}
	
	public static synchronized void printlnSynced(String content, PrintStream printStream)
	{
		printStream.println(content);
	}
}