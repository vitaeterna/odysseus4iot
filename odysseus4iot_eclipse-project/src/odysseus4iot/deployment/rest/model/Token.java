package odysseus4iot.deployment.rest.model;

/**
 * POJO representing a Odysseus-REST Token
 * 
 * @author Michael Sünkel
 */
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