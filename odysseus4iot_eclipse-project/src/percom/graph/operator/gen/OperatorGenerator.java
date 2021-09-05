package percom.graph.operator.gen;

import java.util.ArrayList;
import java.util.List;

import percom.graph.operator.DatabasesourceOperator;
import percom.graph.operator.MapOperator;
import percom.graph.operator.TimewindowOperator;
import percom.graph.operator.meta.Column;
import percom.graph.operator.meta.Schema;

//TODO: Warum kann integer timestamp nicht als starttimestamp verwendet werden?
//TODO: Werden metadaten, wie start und end, immer mitübertragen, auch wenn diese null sind?
public class OperatorGenerator
{
	public DatabasesourceOperator generateDatabasesourceOperator()
	{
		DatabasesourceOperator databasesourceOperator = new DatabasesourceOperator();
		
		databasesourceOperator.table = "sensor_data_19";
		databasesourceOperator.jdbc = "jdbc:postgresql://localhost:5432/CattleDB";
		databasesourceOperator.user = "postgres";
		databasesourceOperator.password = "postgres";
		databasesourceOperator.waiteach = 100;
		
		Schema attributes = new Schema();
		attributes.addColumn(new Column("cattle_id", Integer.class));
		attributes.addColumn(new Column("timestamp", Long.class));
		attributes.addColumn(new Column("ax", Double.class));
		attributes.addColumn(new Column("ay", Double.class));
		attributes.addColumn(new Column("az", Double.class));
		attributes.addColumn(new Column("ox", Double.class));
		attributes.addColumn(new Column("oy", Double.class));
		attributes.addColumn(new Column("oz", Double.class));
		
		databasesourceOperator.attributes = attributes;
		
		databasesourceOperator.outputSchema = databasesourceOperator.attributes;
		databasesourceOperator.outputRate = 10;
		databasesourceOperator.outputName = "sensorData";
		
		return databasesourceOperator;
	}
	
	public MapOperator generateMapOperator()
	{
		MapOperator mapOperator = new MapOperator();
		
		List<String> expressions = new ArrayList<>();
		expressions.add("'cattle_id'");
		expressions.add("'timestamp'");
		expressions.add("['sqrt((ax*ax)+(ay*ay)+(az*az))','amag']");
		expressions.add("['sqrt((ox*ox)+(oy*oy)+(oz*oz))','omag']");
		
		mapOperator.expressions = expressions;
		
		Schema inputSchema = new Schema();
		inputSchema.addColumn(new Column("cattle_id", Integer.class));
		inputSchema.addColumn(new Column("timestamp", Long.class));
		inputSchema.addColumn(new Column("ax", Double.class));
		inputSchema.addColumn(new Column("ay", Double.class));
		inputSchema.addColumn(new Column("az", Double.class));
		inputSchema.addColumn(new Column("ox", Double.class));
		inputSchema.addColumn(new Column("oy", Double.class));
		inputSchema.addColumn(new Column("oz", Double.class));
		
		mapOperator.inputSchema = inputSchema;
		mapOperator.inputRate = 10;
		mapOperator.inputName = "sensorData";
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column("timestamp", Long.class));
		outputSchema.addColumn(new Column("amag", Double.class));
		outputSchema.addColumn(new Column("omag", Double.class));
		
		mapOperator.outputSchema = outputSchema;
		mapOperator.outputRate = 10;
		mapOperator.outputName = "sensorDataMag";
		
		return mapOperator;
	}
	
	public TimewindowOperator generateTimewindowOperator()
	{
		TimewindowOperator timewindowOperator = new TimewindowOperator();
		
		timewindowOperator.size = 5000;
		timewindowOperator.slide = 5000;
		
		Schema inputSchema = new Schema();
		inputSchema.addColumn(new Column("cattle_id", Integer.class));
		inputSchema.addColumn(new Column("timestamp", Long.class));
		inputSchema.addColumn(new Column("amag", Double.class));
		inputSchema.addColumn(new Column("omag", Double.class));
		
		timewindowOperator.inputSchema = inputSchema;
		timewindowOperator.inputRate = 10;
		timewindowOperator.inputName = "sensorDataMag";
		
		Schema outputSchema = new Schema();
		outputSchema.addColumn(new Column("cattle_id", Integer.class));
		outputSchema.addColumn(new Column("timestamp", Long.class));
		outputSchema.addColumn(new Column("amag", Double.class));
		outputSchema.addColumn(new Column("omag", Double.class));
		outputSchema.addColumn(new Column("end_timestamp", Long.class));
		
		timewindowOperator.outputSchema = outputSchema;
		timewindowOperator.outputRate = 10;
		timewindowOperator.outputName = "sensorDataMagWindowed";
		
		return timewindowOperator;
	}
}