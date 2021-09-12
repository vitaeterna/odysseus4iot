package odysseus4iot.graph.operator;

import java.util.List;

import odysseus4iot.graph.operator.meta.Operator;

public class AggregateOperator extends Operator
{
	public String group_by = null;
	public List<String> aggregations = null;
	
	@Override
	public String toString()
	{
		String aggregationsString = "";
		
		String currentAggregation = null;
		
		for(int index = 0; index < this.aggregations.size(); index++)
		{
			currentAggregation = this.aggregations.get(index);
			
			aggregationsString += String.format(QUERY_AGGREGATION, currentAggregation, index==this.aggregations.size()-1?"":",");
		}
		
		return String.format(QUERY, this.outputName, this.group_by, aggregationsString, this.inputName);
	}
	
	public static final String QUERY = 
			  "%s = AGGREGATE\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\tgroup_by = ['%s'],\r\n"
			+ "\t\taggregations=\r\n"
			+ "\t\t[\r\n"
			+ "%s"
			+ "\t\t]\r\n"
			+ "\t},\r\n"
			+ "\t%s\r\n"
			+ ")";
	
	private static final String QUERY_AGGREGATION = 
			  "\t\t\t%s%s\r\n";
}