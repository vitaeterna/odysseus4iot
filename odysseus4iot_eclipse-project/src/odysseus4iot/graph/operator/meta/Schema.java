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
	
	public Long getSize()
	{
		if(this.columns == null)
		{
			return null;
		}
		
		Long size = 0L;
		
		Column currentColumn = null;
		
		for(int index = 0; index < this.columns.size(); index++)
		{
			currentColumn = this.columns.get(index);
			
			Long columnSize = null;
			
			//Not addressed: byte, char, short
			if(currentColumn.type == Boolean.class)
			{
				columnSize = 1L;
			}
			else if(currentColumn.type == Float.class)
			{
				columnSize = (long)Float.SIZE;
			}
			else if(currentColumn.type == Double.class)
			{
				columnSize = (long)Double.SIZE;
			}
			else if(currentColumn.type == Integer.class)
			{
				columnSize = (long)Integer.SIZE;
			}
			else if(currentColumn.type == Long.class || currentColumn.type == StartTimestamp.class)
			{
				columnSize = (long)Long.SIZE;
			}
			else if(currentColumn.type == String.class)
			{
				columnSize = Character.SIZE * 15L;
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
	
	public List<String> toStringList()
	{
		List<String> stringList = new ArrayList<>();
		
		for(int index = 0; index < this.columns.size(); index++)
		{
			stringList.add(this.columns.get(index).name);
		}
		
		return stringList;
	}
	
	@Override
	public String toString()
	{
		return columns.toString();
	}
}