package odysseus4iot.deployment.rest.model;

public class User
{
	private String username = null;
	private String password = null;
	
	public User()
	{
		
	}
	
	public User(String username, String password)
	{
		this.setUsername(username);
		this.setPassword(password);
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