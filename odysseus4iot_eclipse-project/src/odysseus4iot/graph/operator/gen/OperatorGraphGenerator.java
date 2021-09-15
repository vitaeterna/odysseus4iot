package odysseus4iot.graph.operator.gen;

import java.util.ArrayList;
import java.util.List;

import odysseus4iot.graph.Edge;
import odysseus4iot.graph.Graph;
import odysseus4iot.graph.Vertex;
import odysseus4iot.graph.operator.AggregateOperator;
import odysseus4iot.graph.operator.ChangedetectOperator;
import odysseus4iot.graph.operator.ClassificationOperator;
import odysseus4iot.graph.operator.DatabasesinkOperator;
import odysseus4iot.graph.operator.DatabasesourceOperator;
import odysseus4iot.graph.operator.MapOperator;
import odysseus4iot.graph.operator.MergeOperator;
import odysseus4iot.graph.operator.OutlierRemovingOperator;
import odysseus4iot.graph.operator.ProjectOperator;
import odysseus4iot.graph.operator.TimewindowOperator;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.model.Model;

//TODO: project operator generation order (id mixup)
//TODO: generate invisible filler nodes for visualization - project operator
//TODO: handle metadata corretly!
//TODO: write metadata on databasesink
public class OperatorGraphGenerator
{	
	public static Graph generateOperatorGraph(List<String> sensors, List<Model> models, boolean postprocessing)
	{
		if(sensors == null || sensors.isEmpty() || models == null || models.isEmpty())
		{
			return null;
		}
		
		System.out.println("Generation of merged Operator Graph started...");
		
		Graph graph = new Graph();
		
		//1 - DatabasesourceOperator
		Vertex.getNextGroup();
		
		List<DatabasesourceOperator> databasesourceOperators = new ArrayList<>();
		
		DatabasesourceOperator databasesourceOperator = null;
		
		List<String> unionOfSchemata = Model.getUnionOfSchemata(models);
		
		String currentSensor = null;
		
		for(int index = 0; index < sensors.size(); index++)
		{
			currentSensor = sensors.get(index);
			
			databasesourceOperator = OperatorGenerator.generateDatabasesourceOperator(currentSensor, unionOfSchemata, Model.getMinWaiteach(models).intValue());
			
			databasesourceOperators.add(databasesourceOperator);
			
			graph.addVertex(databasesourceOperator);
		}
		
		System.out.println("Generated " + databasesourceOperators.size() + " DatabasesourceOperators");
		
		//1.5 - MergeOperator
		MergeOperator mergeOperator = null;
		
		if(databasesourceOperators.size() > 1)
		{
			Vertex.getNextGroup();
			
			mergeOperator = OperatorGenerator.generateMergeOperator(sensors);
			
			graph.addVertex(mergeOperator);
			
			Double outputRateSum = 0.0d;
			
			for(int index = 0; index < databasesourceOperators.size(); index++)
			{
				databasesourceOperator = databasesourceOperators.get(index);
				
				graph.addEdge(new Edge(databasesourceOperator, mergeOperator));
				
				outputRateSum += databasesourceOperator.outputRate;
			}
			
			mergeOperator.inputSchema = databasesourceOperator.outputSchema.copy();
			mergeOperator.inputRate = outputRateSum;
			mergeOperator.inputName = null;

			mergeOperator.outputSchema = mergeOperator.inputSchema.copy();
			mergeOperator.outputRate = mergeOperator.inputRate;
		}
		
		System.out.println("Generated " + (mergeOperator==null?"0":"1") + " MergeOperators");
		
		//2 - MapOperator
		MapOperator mapOperator = null;
		
		boolean mapOperatorNeeded = false;
		
		List<String> unionOfPreprocessing = Model.getUnionOfPreprocessing(models);
		
		for(int index = 0; index < unionOfPreprocessing.size(); index++)
		{
			if(!unionOfSchemata.contains(unionOfPreprocessing.get(index)))
			{
				mapOperatorNeeded = true;
				
				break;
			}
		}
		
		if(mapOperatorNeeded)
		{
			Vertex.getNextGroup();
			
			mapOperator = OperatorGenerator.generateMapOperator(unionOfPreprocessing);
			
			graph.addVertex(mapOperator);
			
			if(mergeOperator == null)
			{
				graph.addEdge(new Edge(databasesourceOperator, mapOperator));
				
				mapOperator.inputSchema = databasesourceOperator.outputSchema.copy();
				mapOperator.inputRate = databasesourceOperator.outputRate;
				mapOperator.inputName = databasesourceOperator.outputName;
			}
			else
			{
				graph.addEdge(new Edge(mergeOperator, mapOperator));
				
				mapOperator.inputSchema = mergeOperator.outputSchema.copy();
				mapOperator.inputRate = mergeOperator.outputRate;
				mapOperator.inputName = mergeOperator.outputName;
			}
			
			mapOperator.outputRate = mapOperator.inputRate;
		}
		
		System.out.println("Generated " + (mapOperator==null?"0":"1") + " MapOperators");
		
		//3 - TimewindowOperator
		Vertex.getNextGroup();
		Vertex.getNextGroup();
		
		List<ProjectOperator> projectOperators = new ArrayList<>();
		
		List<TimewindowOperator> timewindowOperators = new ArrayList<>();
		
		ProjectOperator projectOperator = null;
		
		TimewindowOperator timewindowOperator = null;
		
		boolean timeWindowExists = false;
		
		Model currentModel = null;
		
		for(int index = 0; index < models.size(); index++)
		{
			currentModel = models.get(index);
			
			timeWindowExists = false;
			
			for(int index2 = 0; index2 < timewindowOperators.size(); index2++)
			{
				timewindowOperator = timewindowOperators.get(index2);
				
				if(timewindowOperator.size.intValue() == currentModel.getWindow_size().intValue() && timewindowOperator.slide.intValue() == currentModel.getWindow_slide().intValue())
				{
					timeWindowExists = true;
					
					timewindowOperator.models.add(currentModel);
					
					break;
				}
			}
			
			if(!timeWindowExists)
			{
				timewindowOperator = OperatorGenerator.generateTimewindowOperator(currentModel.getWindow_size(), currentModel.getWindow_slide());
			
				timewindowOperator.models.add(currentModel);
				
				timewindowOperators.add(timewindowOperator);
				
				graph.addVertex(timewindowOperator);
			}
		}
		
		List<String> currentSchema = null;
		
		Operator previousOperator = null;
		
		if(mapOperator != null)
		{
			previousOperator = mapOperator;
			
			currentSchema = unionOfPreprocessing;
		}
		else
		{
			if(mergeOperator != null)
			{
				previousOperator = mergeOperator;
			}
			else
			{
				previousOperator = (DatabasesourceOperator)graph.getVerticesByType(DatabasesourceOperator.class).get(0);
			}
			
			currentSchema = unionOfSchemata;
		}
		
		for(int index = 0; index < timewindowOperators.size(); index++)
		{
			timewindowOperator = timewindowOperators.get(index);
			
			unionOfPreprocessing = Model.getUnionOfPreprocessing(timewindowOperator.models);
			
			if(unionOfPreprocessing.size() < currentSchema.size())
			{
				projectOperator = OperatorGenerator.generateProjectOperator(unionOfPreprocessing, null);
			
				projectOperators.add(projectOperator);
				
				graph.addVertex(projectOperator);
				
				graph.addEdge(new Edge(previousOperator, projectOperator));
				
				projectOperator.inputSchema = previousOperator.outputSchema.copy();
				projectOperator.inputRate = previousOperator.outputRate;
				projectOperator.inputName = previousOperator.outputName;
				
				projectOperator.outputRate = projectOperator.inputRate;
				
				graph.addEdge(new Edge(projectOperator, timewindowOperator));
				
				timewindowOperator.inputSchema = projectOperator.outputSchema.copy();
				timewindowOperator.inputRate = projectOperator.outputRate;
				timewindowOperator.inputName = projectOperator.outputName;
				
				timewindowOperator.outputSchema = timewindowOperator.inputSchema.copy();
				timewindowOperator.outputRate = timewindowOperator.inputRate;
			}
			else
			{
				graph.addEdge(new Edge(previousOperator, timewindowOperator));
				
				timewindowOperator.inputSchema = previousOperator.outputSchema.copy();
				timewindowOperator.inputRate = previousOperator.outputRate;
				timewindowOperator.inputName = previousOperator.outputName;
				
				timewindowOperator.outputSchema = timewindowOperator.inputSchema.copy();
				timewindowOperator.outputRate = timewindowOperator.inputRate;
			}
		}
		
		for(int index = 0; index < projectOperators.size(); index++)
		{
			projectOperator = projectOperators.get(index);
			
			projectOperator.group -= 1;
		}
		
		System.out.println("Generated " + projectOperators.size() + " ProjectOperators");
		
		System.out.println("Generated " + timewindowOperators.size() + " TimewindowOperators");
		
		//5 - AggregateOperator
		Vertex.getNextGroup();
		
		List<AggregateOperator> aggregateOperators = new ArrayList<>();
		
		AggregateOperator aggregateOperator = null;
		
		for(int index = 0; index < timewindowOperators.size(); index++)
		{
			timewindowOperator = timewindowOperators.get(index);
			
			aggregateOperator = OperatorGenerator.generateAggregateOperator(Model.getUnionOfFeatures(timewindowOperator.models));
			
			aggregateOperator.models = timewindowOperator.models;
			
			aggregateOperators.add(aggregateOperator);
			
			graph.addVertex(aggregateOperator);
			
			graph.addEdge(new Edge(timewindowOperator, aggregateOperator));
			
			aggregateOperator.inputSchema = timewindowOperator.outputSchema.copy();
			aggregateOperator.inputRate = timewindowOperator.outputRate;
			aggregateOperator.inputName = timewindowOperator.outputName;
			
			//TODO: consider window_slide
			aggregateOperator.outputRate = aggregateOperator.inputRate/(((double)timewindowOperator.size)/databasesourceOperator.waiteach);
		}
		
		System.out.println("Generated " + aggregateOperators.size() + " AggregateOperators");
		
		//6 - ClassificationOperator
		Vertex.getNextGroup();
		Vertex.getNextGroup();
		
		List<ClassificationOperator> classificationOperators = new ArrayList<>();
		
		ClassificationOperator classificationOperator = null;
		
		List<String> unionOfFeatures = null;
		
		List<String> outputStreams = new ArrayList<>();
		
		projectOperators.clear();
		
		for(int index = 0; index < aggregateOperators.size(); index++)
		{
			aggregateOperator = aggregateOperators.get(index);
			
			unionOfFeatures = Model.getUnionOfFeatures(aggregateOperator.models);
			
			boolean projectOperatorExists = false;
			
			for(int index2 = 0; index2 < aggregateOperator.models.size(); index2++)
			{
				currentModel = models.get(index2);
				
				classificationOperator = OperatorGenerator.generateClassificationOperator(currentModel.getModel_title());
				
				classificationOperator.models.add(currentModel);
				
				classificationOperators.add(classificationOperator);
				
				graph.addVertex(classificationOperator);
				
				outputStreams.add(classificationOperator.outputName);
				
				//IsProjectNeeded
				if(currentModel.getFeatures().size() != unionOfFeatures.size())
				{
					projectOperatorExists = false;
					
					for(int index3 = 0; index3 < projectOperators.size(); index3++)
					{
						projectOperator = projectOperators.get(index3);
						
						if(projectOperator.payloadAttributes.containsAll(currentModel.getFeatures()) && projectOperator.payloadAttributes.size() == currentModel.getFeatures().size())
						{
							projectOperatorExists = true;
							
							projectOperator.models.add(currentModel);
							
							break;
						}
					}
					
					if(!projectOperatorExists)
					{
						projectOperator = OperatorGenerator.generateProjectOperator(currentModel.getFeatures(), "car_");
					
						projectOperator.models.add(currentModel);
						
						projectOperators.add(projectOperator);
						
						graph.addVertex(projectOperator);
						
						graph.addEdge(new Edge(aggregateOperator, projectOperator));
						
						projectOperator.inputSchema = aggregateOperator.outputSchema.copy();
						projectOperator.inputRate = aggregateOperator.outputRate;
						projectOperator.inputName = aggregateOperator.outputName;
						
						projectOperator.outputRate = projectOperator.inputRate;
					}
					
					graph.addEdge(new Edge(projectOperator, classificationOperator));
					
					classificationOperator.inputSchema = projectOperator.outputSchema.copy();
					classificationOperator.inputRate = projectOperator.outputRate;
					classificationOperator.inputName = projectOperator.outputName;
					
					classificationOperator.outputRate = classificationOperator.inputRate;
				}
				else
				{
					//ProjectNotNeeded
					graph.addEdge(new Edge(aggregateOperator, classificationOperator));
					
					classificationOperator.inputSchema = aggregateOperator.outputSchema.copy();
					classificationOperator.inputRate = aggregateOperator.outputRate;
					classificationOperator.inputName = aggregateOperator.outputName;
					
					classificationOperator.outputRate = classificationOperator.inputRate;
				}
			}
		}
		
		for(int index = 0; index < projectOperators.size(); index++)
		{
			projectOperator = projectOperators.get(index);
			
			projectOperator.group -= 1;
		}
		
		System.out.println("Generated " + projectOperators.size() + " ProjectOperators");
		
		System.out.println("Generated " + classificationOperators.size() + " ClassificationOperators");
		
		//7+8 - Postprocessing
		List<OutlierRemovingOperator> outlierRemovingOperators = new ArrayList<>();
		
		OutlierRemovingOperator outlierRemovingOperator = null;
		
		List<ChangedetectOperator> changedetectOperators = new ArrayList<>();
		
		ChangedetectOperator changedetectOperator = null;
		
		if(postprocessing)
		{
			outputStreams.clear();
			
			//7 - OutlierRemovingOperator
			Vertex.getNextGroup();
			
			for(int index = 0; index < classificationOperators.size(); index++)
			{
				classificationOperator = classificationOperators.get(index);
				
				outlierRemovingOperator = OperatorGenerator.generateOutlierRemovingOperator(classificationOperator.models.get(0).getModel_title());
				
				outlierRemovingOperator.models.add(classificationOperator.models.get(0));
				
				outlierRemovingOperators.add(outlierRemovingOperator);
				
				graph.addVertex(outlierRemovingOperator);
				
				graph.addEdge(new Edge(classificationOperator, outlierRemovingOperator));
				
				outlierRemovingOperator.inputSchema = classificationOperator.outputSchema.copy();
				outlierRemovingOperator.inputRate = classificationOperator.outputRate;
				outlierRemovingOperator.inputName = classificationOperator.outputName;
				
				outlierRemovingOperator.outputSchema = outlierRemovingOperator.inputSchema.copy();
				outlierRemovingOperator.outputRate = outlierRemovingOperator.inputRate;
			}
			
			System.out.println("Generated " + outlierRemovingOperators.size() + " OutlierRemovingOperators");
			
			//8 - ChangedetectOperator
			Vertex.getNextGroup();
			
			for(int index = 0; index < outlierRemovingOperators.size(); index++)
			{
				outlierRemovingOperator = outlierRemovingOperators.get(index);
				
				changedetectOperator = OperatorGenerator.generateChangedetectOperator(outlierRemovingOperator.models.get(0).getModel_title());
				
				changedetectOperator.models.add(outlierRemovingOperator.models.get(0));
				
				changedetectOperators.add(changedetectOperator);
				
				graph.addVertex(changedetectOperator);
				
				outputStreams.add(changedetectOperator.outputName);
				
				graph.addEdge(new Edge(outlierRemovingOperator, changedetectOperator));
				
				changedetectOperator.inputSchema = outlierRemovingOperator.outputSchema.copy();
				changedetectOperator.inputRate = outlierRemovingOperator.outputRate;
				changedetectOperator.inputName = outlierRemovingOperator.outputName;
				
				changedetectOperator.outputSchema = changedetectOperator.inputSchema.copy();
				changedetectOperator.outputRate = changedetectOperator.inputRate;
			}
			
			System.out.println("Generated " + changedetectOperators.size() + " ChangedetectOperators");
		}
		
		//8.5 - MergeOperator
		mergeOperator = null;
		
		if(!postprocessing && classificationOperators.size() > 1)
		{
			Vertex.getNextGroup();
			
			mergeOperator = OperatorGenerator.generateMergeOperator(outputStreams);
			
			graph.addVertex(mergeOperator);
			
			Double outputRateSum = 0.0d;
			
			for(int index = 0; index < classificationOperators.size(); index++)
			{
				classificationOperator = classificationOperators.get(index);
				
				graph.addEdge(new Edge(classificationOperator, mergeOperator));
				
				outputRateSum += classificationOperator.outputRate;
			}
			
			mergeOperator.inputSchema = classificationOperator.outputSchema.copy();
			mergeOperator.inputRate = outputRateSum;
			mergeOperator.inputName = null;

			mergeOperator.outputSchema = mergeOperator.inputSchema.copy();
			mergeOperator.outputRate = mergeOperator.inputRate;
		}
		
		if(postprocessing && changedetectOperators.size() > 1)
		{
			Vertex.getNextGroup();
			
			mergeOperator = OperatorGenerator.generateMergeOperator(outputStreams);
			
			graph.addVertex(mergeOperator);
			
			Double outputRateSum = 0.0d;
			
			for(int index = 0; index < changedetectOperators.size(); index++)
			{
				changedetectOperator = changedetectOperators.get(index);
				
				graph.addEdge(new Edge(changedetectOperator, mergeOperator));
				
				outputRateSum += changedetectOperator.outputRate;
			}
			
			mergeOperator.inputSchema = changedetectOperator.outputSchema.copy();
			mergeOperator.inputRate = outputRateSum;
			mergeOperator.inputName = null;

			mergeOperator.outputSchema = mergeOperator.inputSchema.copy();
			mergeOperator.outputRate = mergeOperator.inputRate;
		}
		
		System.out.println("Generated " + (mergeOperator==null?"0":"1") + " MergeOperators");
		
		//9 - DatabasesinkOperator
		if(mergeOperator != null)
		{
			previousOperator = mergeOperator;
		}
		else
		{
			if(postprocessing)
			{
				previousOperator = changedetectOperator;
			}
			else
			{
				previousOperator = classificationOperator;
			}
		}
		
		Vertex.getNextGroup();
		
		DatabasesinkOperator databasesinkOperator = OperatorGenerator.generateDatabasesinkOperator("prediction");
		
		graph.addVertex(databasesinkOperator);
		
		graph.addEdge(new Edge(previousOperator, databasesinkOperator));
		
		databasesinkOperator.inputSchema = previousOperator.outputSchema.copy();
		databasesinkOperator.inputRate = previousOperator.outputRate;
		databasesinkOperator.inputName = previousOperator.outputName;
		
		databasesinkOperator.outputSchema = databasesinkOperator.inputSchema.copy();
		databasesinkOperator.outputRate = databasesinkOperator.inputRate;
		
		System.out.println("Generated 1 DatabasesinkOperator");
		
		//Adding labels to edges
		Edge currentEdge = null;
		
		for(int index = 0; index < graph.edges.size(); index++)
		{
			currentEdge = graph.edges.get(index);
			
			if(((Operator)currentEdge.vertex0).outputSchema.columns.size() > 10)
			{
				currentEdge.label = Integer.toString(((Operator)currentEdge.vertex0).outputSchema.columns.size());
			}
			else
			{
				currentEdge.label = ((Operator)currentEdge.vertex0).outputSchema.toString();
			}
			
			currentEdge.label += " | " + ((Operator)currentEdge.vertex0).outputRate + "Hz";
		}
		
		System.out.println("...Generation of merged Operator Graph finished");
		
		return graph;
	}
}