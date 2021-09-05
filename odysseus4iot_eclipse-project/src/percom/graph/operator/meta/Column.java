package percom.graph.operator.meta;

public class Column
{
	public String name = null;
	public Class<? extends Object> type = null;
	
	public Column()
	{
		
	}
	
	public Column(String name, Class<? extends Object> type)
	{
		this.name = name;
		this.type = type;
	}
}