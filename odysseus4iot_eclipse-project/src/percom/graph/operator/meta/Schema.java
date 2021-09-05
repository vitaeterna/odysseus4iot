package percom.graph.operator.meta;

import java.util.List;

public class Schema
{
	public List<Column> columns = null;
	
	public int getSize()
	{
		if(this.columns == null)
		{
			return -1;
		}
		
		int size = 0;
		
		Column currentColumn = null;
		
		for(int index = 0; index < this.columns.size(); index++)
		{
			currentColumn = this.columns.get(index);
			
			int columnSize = -1;
			
			//Not addressed: byte, char, short
			if(currentColumn.type == Boolean.class)
			{
				columnSize = 1;
			}
			else if(currentColumn.type == Float.class)
			{
				columnSize = Float.SIZE;
			}
			else if(currentColumn.type == Double.class)
			{
				columnSize = Double.SIZE;
			}
			else if(currentColumn.type == Integer.class)
			{
				columnSize = Integer.SIZE;
			}
			else if(currentColumn.type == Long.class)
			{
				columnSize = Long.SIZE;
			}
			else if(currentColumn.type == String.class)
			{
				//TODO: Average String length?
				columnSize = Character.SIZE * 20;
			}
			
			size += columnSize;
		}
		
		return size;
	}
}