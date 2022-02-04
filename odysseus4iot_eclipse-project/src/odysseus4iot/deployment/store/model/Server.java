package odysseus4iot.deployment.store.model;

/**
 * POJO representing a Odysseus Server (socket and credentials).
 * 
 * @author Michael Sünkel
 */
public class Server
{
	private String socket = null;
	private String username = null;
	private String password = null;
	
	public Server()
	{
		
	}
	
	public String getSocket()
	{
		return this.socket;
	}
	
	public void setSocket(String socket)
	{
		this.socket = socket;
	}
	
	public String getUsername()
	{
		return this.username;
	}
	
	public void setUsername(String username)
	{
		this.username = username;
	}
	
	public String getPassword()
	{
		return this.password;
	}
	
	public void setPassword(String password)
	{
		this.password = password;
	}
}