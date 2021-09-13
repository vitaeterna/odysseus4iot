package odysseus4iot.graph.operator.meta;

import java.util.ArrayList;
import java.util.List;

public class Schema
{
	public List<Column> columns = null;
	
	public void addColumn(Column column)
	{
		if(this.columns == null)
		{
			this.columns = new ArrayList<>();
		}
		
		this.columns.add(column);
	}
	
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
	
	public Schema copy()
	{
		Schema schema = new Schema();
		
		if(this.columns != null)
		{
			List<Column> columns = new ArrayList<>();
			
			for(int index = 0; index < this.columns.size(); index++)
			{
				columns.add(this.columns.get(index).copy());
			}
			
			schema.columns = columns;
		}
		
		return schema;
	}
	
	@Override
	public String toString()
	{
		return columns.toString();
	}
}