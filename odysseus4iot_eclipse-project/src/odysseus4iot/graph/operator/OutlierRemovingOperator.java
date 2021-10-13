package odysseus4iot.graph.operator;

import odysseus4iot.graph.operator.meta.Operator;

public class OutlierRemovingOperator extends Operator
{
	public String group_by = null;
	public String model_title = null;
	
	public OutlierRemovingOperator()
	{
		super();
		
		type = Type.PROCESSING;
	}
	
	public OutlierRemovingOperator(Operator operator)
	{
		super(operator);
	}
	
	public OutlierRemovingOperator copy()
	{
		OutlierRemovingOperator operator = new OutlierRemovingOperator(super.copy());
		
		operator.group_by = this.group_by;
		operator.model_title = this.model_title;
		
		return operator;
	}
	
	@Override
	public String toString()
	{
		return String.format(QUERY, this.model_title, this.group_by, this.inputName, this.model_title, this.group_by, this.model_title, this.model_title, this.model_title, this.model_title, this.model_title, this.outputName, this.model_title);
	}
	
	public static final String QUERY = 
			  "outlier_window_%s = ELEMENTWINDOW({SIZE = 2, ADVANCE = 1, DRAINATDONE = true, PARTITION = ['%s']}, %s)\r\n"
			+ "\r\n"
			+ "outlier_aggregate_%s = AGGREGATE({AGGREGATIONS = [['nest', '*', 'nested']], GROUP_BY = ['%s']}, outlier_window_%s)\r\n"
			+ "\r\n"
			+ "outlier_select_%s = SELECT({PREDICATE = 'size(nested) = 2 && elementAt(nested[0],1) = elementAt(nested[1],1)'}, outlier_aggregate_%s)\r\n"
			+ "\r\n"
			+ "outlier_map_%s = MAP({EXPRESSIONS = [['nested[0]','nested']]}, outlier_select_%s)\r\n"
			+ "\r\n"
			+ "%s = UNNEST({ATTRIBUTE = 'nested'}, outlier_map_%s)";
}