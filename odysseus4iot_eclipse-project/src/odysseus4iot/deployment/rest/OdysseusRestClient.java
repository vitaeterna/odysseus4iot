package odysseus4iot.deployment.rest;

import java.io.IOException;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import odysseus4iot.deployment.GlobalQueryScript;
import odysseus4iot.deployment.json.OdysseusJsonClient;
import odysseus4iot.deployment.rest.model.Query;
import odysseus4iot.deployment.rest.model.Token;
import odysseus4iot.deployment.rest.model.User;
import odysseus4iot.deployment.store.model.PartialQuery;

public class OdysseusRestClient
{
	public static final String URI_PROTOCOL = "http://";
	public static final String URI_LOGIN    = "/services/login";
	public static final String URI_QUERY    = "/queries";
	
	public static final Integer timeout     = 15;
	
	public static Token post_login(String socket, User user)
	{
	    //Create and configure a HTTP Request
	    HttpRequest httpRequest = null;
	    
	    HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder();
	    
	    httpRequestBuilder.uri(URI.create(URI_PROTOCOL + socket + URI_LOGIN));
	    httpRequestBuilder.version(HttpClient.Version.HTTP_2);
	    httpRequestBuilder.timeout(Duration.of(timeout, ChronoUnit.SECONDS));
	    httpRequestBuilder.header("Content-Type", "application/json");
	    httpRequestBuilder.POST(HttpRequest.BodyPublishers.ofString(OdysseusJsonClient.getJsonFromUser(user)));
	    
	    httpRequest = httpRequestBuilder.build();
	    
	    //Create and configure a HTTP Client
	    HttpClient httpClient = null;
	    
	    HttpClient.Builder httpClientBuilder = HttpClient.newBuilder();
	    
	    httpClientBuilder.proxy(ProxySelector.getDefault());
	    httpClientBuilder.followRedirects(Redirect.NEVER);
	    httpClientBuilder.cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_NONE));
	    
	    httpClient = httpClientBuilder.build();
	    
	    //Send HTTP Request using HTTP Client and receiving HTTP Response
	    HttpResponse<String> httpResponse = null;
	    
	    try
	    {
	    	httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
	    }
	    catch(IOException | InterruptedException e)
	    {
			GlobalQueryScript.printlnSynced(e.getClass().getName() + ": " + e.getMessage(), System.err);
			
			return null;
	    }

	    if(httpResponse.statusCode() == 200)
	    {
	    	return OdysseusJsonClient.getTokenFromJson(httpResponse.body());
	    }
	    else
	    {
	    	GlobalQueryScript.printlnSynced("http status code: " + httpResponse.statusCode(), System.err);
	    	
	    	return null;
	    }
	}
	
	public static boolean post_query(String socket, Token token, Query query, PartialQuery partialQuery)
	{
	    //Create and configure a HTTP Request
	    HttpRequest httpRequest = null;
	    
	    HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder();
	    
	    httpRequestBuilder.uri(URI.create(URI_PROTOCOL + socket + URI_QUERY));
	    httpRequestBuilder.version(HttpClient.Version.HTTP_2);
	    httpRequestBuilder.timeout(Duration.of(timeout, ChronoUnit.SECONDS));
	    httpRequestBuilder.header("Content-Type", "application/json");
	    httpRequestBuilder.header("Authorization", "Bearer " + token.getToken());
	    httpRequestBuilder.POST(HttpRequest.BodyPublishers.ofString(OdysseusJsonClient.getJsonFromQuery(query)));
	    
	    httpRequest = httpRequestBuilder.build();
	    
	    //Create and configure a HTTP Client
	    HttpClient httpClient = null;
	    
	    HttpClient.Builder httpClientBuilder = HttpClient.newBuilder();
	    
	    httpClientBuilder.proxy(ProxySelector.getDefault());
	    httpClientBuilder.followRedirects(Redirect.NEVER);
	    httpClientBuilder.cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_NONE));
	    
	    httpClient = httpClientBuilder.build();
	    
	    //Send HTTP Request using HTTP Client and receiving HTTP Response
	    HttpResponse<String> httpResponse = null;
	    
	    try
	    {
	    	httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
	    }
	    catch(IOException | InterruptedException e)
	    {
			GlobalQueryScript.printlnSynced(e.getClass().getName() + ": " + e.getMessage(), System.err);
			
			return false;
	    }
	    
	    if(httpResponse.statusCode() == 200)
	    {
	    	List<Query> queries = OdysseusJsonClient.getQueriesFromJson(httpResponse.body());
	    	
	    	partialQuery.setQueries(queries);
	    	
	    	return true;
	    }
	    else
	    {
	    	GlobalQueryScript.printlnSynced("http status code: " + httpResponse.statusCode(), System.err);
	    	
	    	return false;
	    }
	}
	
	public static boolean delete_query(String socket, Token token, Query query, PartialQuery partialQuery)
	{
	    //Create and configure a HTTP Request
	    HttpRequest httpRequest = null;
	    
	    HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder();
	    
	    httpRequestBuilder.uri(URI.create(URI_PROTOCOL + socket + URI_QUERY + "/" + query.getId()));
	    httpRequestBuilder.version(HttpClient.Version.HTTP_2);
	    httpRequestBuilder.timeout(Duration.of(timeout, ChronoUnit.SECONDS));
	    httpRequestBuilder.header("Content-Type", "application/json");
	    httpRequestBuilder.header("Authorization", "Bearer " + token.getToken());
	    httpRequestBuilder.DELETE();
	    
	    httpRequest = httpRequestBuilder.build();
	    
	    //Create and configure a HTTP Client
	    HttpClient httpClient = null;
	    
	    HttpClient.Builder httpClientBuilder = HttpClient.newBuilder();
	    
	    httpClientBuilder.proxy(ProxySelector.getDefault());
	    httpClientBuilder.followRedirects(Redirect.NEVER);
	    httpClientBuilder.cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_NONE));
	    
	    httpClient = httpClientBuilder.build();
	    
	    //Send HTTP Request using HTTP Client and receiving HTTP Response
	    HttpResponse<Void> httpResponse = null;
	    
	    try
	    {
	    	httpResponse = httpClient.send(httpRequest, BodyHandlers.discarding());
	    }
	    catch(IOException | InterruptedException e)
	    {
			GlobalQueryScript.printlnSynced(e.getClass().getName() + ": " + e.getMessage(), System.err);
			
			return false;
	    }
	    
	    if(httpResponse.statusCode() == 200)
	    {
	    	partialQuery.removeQuery(query);
	    	
	    	return true;
	    }
	    else
	    {
	    	GlobalQueryScript.printlnSynced("http status code: " + httpResponse.statusCode(), System.err);
	    	
	    	return false;
	    }
	}
	
	public static void printDebugInfo(HttpResponse<String> httpResponse)
	{
	    GlobalQueryScript.printlnSynced("http url: " + httpResponse.uri().getScheme() + "://" + httpResponse.uri().getHost() + ":" + httpResponse.uri().getPort() + httpResponse.uri().getPath(), System.out);
	    
	    GlobalQueryScript.printlnSynced("http status code: " + httpResponse.statusCode(), System.out);
	    
	    Map<String,List<String>> httpHeadersMap = httpResponse.headers().map();
	    
	    for(Entry<String, List<String>> httpHeader : httpHeadersMap.entrySet())
	    {
	    	GlobalQueryScript.printlnSynced(httpHeader.getKey() + ": " + httpHeader.getValue(), System.out);
	    }
	    
	    GlobalQueryScript.printlnSynced("body: " + httpResponse.body(), System.out);
	}
}