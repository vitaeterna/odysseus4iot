package odysseus4iot.graph;

public class Vertex
{
	private static Integer currentID = 0;
	private static Integer currentGroup = 0;
	
	public static Integer getCurrentID()
	{
		return currentID;
	}
	
	public static Integer getNextID()
	{
		return ++currentID;
	}
	
	public static Integer getCurrentGroup()
	{
		return currentGroup;
	}
	
	public static Integer getNextGroup()
	{
		return ++currentGroup;
	}
	
	public static void resetIDs()
	{
		currentID = 0;
		currentGroup = 0;
	}
	
	public Vertex()
	{
		this.id = getNextID();
		this.group = getCurrentGroup();
	}
	
	public Integer id = null;
	public Integer group = null;
	public String label = null;
}