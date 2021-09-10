package odysseus4iot.deployment.rest.model;

public class Token
{
	private String token = null;
	
	public Token()
	{
		
	}
	
	public Token(String token)
	{
		this.setToken(token);
	}
	
	public String getToken()
	{
		return this.token;
	}
	
	public void setToken(String token)
	{
		this.token = token;
	}
}