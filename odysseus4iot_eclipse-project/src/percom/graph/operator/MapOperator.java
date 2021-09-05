package percom.graph.operator;

import percom.graph.operator.meta.Operator;

public class MapOperator extends Operator
{
	public static final String QUERY = 
			  "sensorMagnitudes = MAP\r\n"
			+ "(\r\n"
			+ "\t{\r\n"
			+ "\t\texpressions =\r\n"
			+ "\t\t[\r\n"
			+ "\t\t\t'timestamp',\r\n"
			+ "\t\t\t'cattle_id',\r\n"
			+ "\t\t\t['sqrt((ax*ax)+(ay*ay)+(az*az))','Amag'],\r\n"
			+ "\t\t\t['sqrt((gx*gx)+(gy*gy)+(gz*gz))','Gmag']\r\n"
			+ "\t\t]\r\n"
			+ "\t},\r\n"
			+ "\tsensorDataRate\r\n"
			+ ")";
}