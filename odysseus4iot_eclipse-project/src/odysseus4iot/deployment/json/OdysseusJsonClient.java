package odysseus4iot.deployment.json;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.gson.Gson;

import odysseus4iot.deployment.rest.model.Query;
import odysseus4iot.deployment.rest.model.Token;
import odysseus4iot.deployment.rest.model.User;
import odysseus4iot.deployment.store.model.GlobalQuery;

/**
 * A simple JSON-POJO converter for Odysseus-REST API
 * 
 * @author Michael Sünkel
 */
public class OdysseusJsonClient
{
	public static Token getTokenFromJson(String json)
	{
		return new Gson().fromJson(json, Token.class);
	}
	
	public static List<Query> getQueriesFromJson(String json)
	{
		return new ArrayList<Query>(Arrays.asList(new Gson().fromJson(json, Query[].class)));
	}
	
	public static GlobalQuery getGlobalQueryFromJson(String json)
	{
		return new Gson().fromJson(json, GlobalQuery.class);
	}
	
	public static String getJsonFromUser(User user)
	{
		return new Gson().toJson(user);
	}
	
	public static String getJsonFromQuery(Query query)
	{
		return new Gson().toJson(query);
	}
}