package odysseus4iot.graph.operator;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.operator.meta.Operator;

/**
 * POJO representing the Odysseus Operator AGGREGATE.
 * 
 * @author Michael Sünkel
 */
public class AggregateOperator extends Operator
{
	public String group_by = null;
	public List<String> aggregations = null;
	
	public AggregateOperator()
	{
		super();
		
		type = Type.PROCESSING;
	}
	
	public AggregateOperator(Operator operator)
	{
		super(operator);
	}
	
	public AggregateOperator copy()
	{
		AggregateOperator operator = new AggregateOperator(super.copy());
		
		operator.group_by = this.group_by;
		operator.aggregations = new ArrayList<>(this.aggregations);
		
		return operator;
	}
	
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