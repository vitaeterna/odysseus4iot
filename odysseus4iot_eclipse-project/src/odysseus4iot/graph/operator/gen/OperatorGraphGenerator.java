package odysseus4iot.graph.operator.gen;

import java.util.ArrayList;
import java.util.List;

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
import odysseus4iot.graph.operator.meta.DataFlow;
import odysseus4iot.graph.operator.meta.Operator;
import odysseus4iot.graph.operator.meta.OperatorGraph;
import odysseus4iot.model.Feature;
import odysseus4iot.model.Model;
import odysseus4iot.util.Util;

//TODO: ___ project operator generation order (id mixup)
//TODO: ___ generate invisible filler nodes for visualization - project operator
//TODO: ___ handle metadata correctly!
//TODO: ___ write metadata on databasesink
public class OperatorGraphGenerator
{	
	public static OperatorGraph generateOperatorGraph(List<String> sensors, List<Model> models, boolean postprocessing, boolean merge)
	{
		if(sensors == null || sensors.isEmpty() || models == null || models.isEmpty())
		{
			return null;
		}
		
		Long startTimestamp = System.currentTimeMillis();
		Long endTimestamp = null;
		
		OperatorGraph operatorGraph = null;
		
		if(models.size() == 1)
		{
			System.out.println("Generation of Operator Graph for model " + models.get(0).getModel_title() + " started...");
			
			operatorGraph = new OperatorGraph(models.get(0).getModel_title());
		}
		else
		{
			System.out.println("Generation of Merged Operator Graph for models=" + models + " started...");
			
			operatorGraph = new OperatorGraph("merged");
		}
		
		Operator previousOperator = null;
		
		List<Operator> previousOperators = new ArrayList<>();
		
		Double outputRateSum = 0.0d;
		
		//1 - DatabasesourceOperator
		operatorGraph.getNextGroup();
		
		List<DatabasesourceOperator> databasesourceOperators = new ArrayList<>();
		
		DatabasesourceOperator databasesourceOperator = null;
		
		List<String> unionOfSchemata = Model.getUnionOfSchemata(models);
		
		String currentSensor = null;
		
		for(int index = 0; index < sensors.size(); index++)
		{
			currentSensor = sensors.get(index);
			
			databasesourceOperator = OperatorGenerator.generateDatabasesourceOperator(currentSensor, unionOfSchemata, Model.getMinWaiteach(models).intValue());
			
			databasesourceOperators.add(databasesourceOperator);
			
			operatorGraph.addVertex(databasesourceOperator);
		}
		
		previousOperators.clear();
		previousOperators.addAll(databasesourceOperators);
		previousOperator = databasesourceOperators.get(0);
		
		System.out.println("Generated " + databasesourceOperators.size() + " DatabasesourceOperators");
		
		//1.5 - MergeOperator
		MergeOperator mergeOperator = null;
		
		if(merge)
		{
			if(previousOperators.size() > 1)
			{
				operatorGraph.getNextGroup();
				
				mergeOperator = OperatorGenerator.generateMergeOperator(sensors);
				
				operatorGraph.addVertex(mergeOperator);
				
				outputRateSum = 0.0d;
				
				for(int index = 0; index < previousOperators.size(); index++)
				{
					previousOperator = previousOperators.get(index);
					
					operatorGraph.addEdge(new DataFlow(previousOperator, mergeOperator));
					
					outputRateSum += previousOperator.outputRate;
				}
				
				mergeOperator.inputSchema = previousOperator.outputSchema.copy();
				mergeOperator.inputRate = outputRateSum;
				mergeOperator.inputName = null;
	
				mergeOperator.outputSchema = mergeOperator.inputSchema.copy();
				mergeOperator.outputRate = mergeOperator.inputRate;
				
				previousOperators.clear();
				previousOperators.add(mergeOperator);
				previousOperator = mergeOperator;
			}
			
			System.out.println("Generated " + (mergeOperator==null?"0":"1") + " MergeOperators");
		}
		
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
		
		if(mapOperatorNeeded || mergeOperator == null)
		{
			operatorGraph.getNextGroup();
			
			mapOperator = OperatorGenerator.generateMapOperator(unionOfPreprocessing);
			
			operatorGraph.addVertex(mapOperator);
			
			outputRateSum = 0.0d;
			
			for(int index = 0; index < previousOperators.size(); index++)
			{
				previousOperator = previousOperators.get(index);
				
				operatorGraph.addEdge(new DataFlow(previousOperator, mapOperator));
				
				outputRateSum += previousOperator.outputRate;
			}
			
			mapOperator.inputSchema = previousOperator.outputSchema.copy();
			mapOperator.inputRate = outputRateSum;
			mapOperator.inputName = previousOperators.size()==1?previousOperator.outputName:"undefined";
			
			mapOperator.outputRate = mapOperator.inputRate;
			
			previousOperators.clear();
			previousOperators.add(mapOperator);
			previousOperator = mapOperator;
		}
		
		System.out.println("Generated " + (mapOperator==null?"0":"1") + " MapOperators");
		
		//3 - TimewindowOperator
		operatorGraph.getNextGroup();
		operatorGraph.getNextGroup();
		
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
				
				operatorGraph.addVertex(timewindowOperator);
			}
		}
		
		List<String> currentSchema = null;
		
		if(previousOperator == mapOperator)
		{
			currentSchema = unionOfPreprocessing;
		}
		else
		{
			currentSchema = unionOfSchemata;
		}
		
		for(int index = 0; index < timewindowOperators.size(); index++)
		{
			timewindowOperator = timewindowOperators.get(index);
			
			unionOfPreprocessing = Model.getUnionOfPreprocessing(timewindowOperator.models);
			
			if(unionOfPreprocessing.size() < currentSchema.size())
			{
				projectOperator = OperatorGenerator.generateProjectOperator(Feature.getFeaturesFromNames(unionOfPreprocessing), null);
			
				projectOperators.add(projectOperator);
				
				operatorGraph.addVertex(projectOperator);
				
				operatorGraph.addEdge(new DataFlow(previousOperator, projectOperator));
				
				projectOperator.inputSchema = previousOperator.outputSchema.copy();
				projectOperator.inputRate = previousOperator.outputRate;
				projectOperator.inputName = previousOperator.outputName;
				
				projectOperator.outputRate = projectOperator.inputRate;
				
				operatorGraph.addEdge(new DataFlow(projectOperator, timewindowOperator));
				
				timewindowOperator.inputSchema = projectOperator.outputSchema.copy();
				timewindowOperator.inputRate = projectOperator.outputRate;
				timewindowOperator.inputName = projectOperator.outputName;
				
				timewindowOperator.outputSchema = timewindowOperator.inputSchema.copy();
				timewindowOperator.outputRate = timewindowOperator.inputRate;
			}
			else
			{
				operatorGraph.addEdge(new DataFlow(previousOperator, timewindowOperator));
				
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
		
		previousOperators.clear();
		previousOperators.addAll(timewindowOperators);
		previousOperator = timewindowOperators.get(0);
		
		System.out.println("Generated " + projectOperators.size() + " ProjectOperators");
		
		System.out.println("Generated " + timewindowOperators.size() + " TimewindowOperators");
		
		//5 - AggregateOperator
		operatorGraph.getNextGroup();
		
		List<AggregateOperator> aggregateOperators = new ArrayList<>();
		
		AggregateOperator aggregateOperator = null;
		
		for(int index = 0; index < previousOperators.size(); index++)
		{
			previousOperator = previousOperators.get(index);
			
			aggregateOperator = OperatorGenerator.generateAggregateOperator(Model.getUnionOfFeatures(previousOperator.models));
			
			aggregateOperator.models = previousOperator.models;
			
			aggregateOperators.add(aggregateOperator);
			
			operatorGraph.addVertex(aggregateOperator);
			
			operatorGraph.addEdge(new DataFlow(previousOperator, aggregateOperator));
			
			aggregateOperator.inputSchema = previousOperator.outputSchema.copy();
			aggregateOperator.inputRate = previousOperator.outputRate;
			aggregateOperator.inputName = previousOperator.outputName;
			
			//TODO: ___ consider window_slide
			aggregateOperator.outputRate = aggregateOperator.inputRate/(((double)((TimewindowOperator)previousOperator).size)/databasesourceOperator.waiteach);
		}
		
		previousOperators.clear();
		previousOperators.addAll(aggregateOperators);
		previousOperator = aggregateOperators.get(0);
		
		System.out.println("Generated " + aggregateOperators.size() + " AggregateOperators");
		
		//6 - ClassificationOperator
		operatorGraph.getNextGroup();
		operatorGraph.getNextGroup();
		
		List<ClassificationOperator> classificationOperators = new ArrayList<>();
		
		ClassificationOperator classificationOperator = null;
		
		List<Feature> unionOfFeatures = null;
		
		List<String> outputStreams = new ArrayList<>();
		
		projectOperators.clear();
		
		for(int index = 0; index < previousOperators.size(); index++)
		{
			previousOperator = previousOperators.get(index);
			
			unionOfFeatures = Model.getUnionOfFeatures(previousOperator.models);
			
			boolean projectOperatorExists = false;
			
			for(int index2 = 0; index2 < previousOperator.models.size(); index2++)
			{
				currentModel = models.get(index2);
				
				classificationOperator = OperatorGenerator.generateClassificationOperator(currentModel.getModel_title());
				
				classificationOperator.models.add(currentModel);
				
				classificationOperators.add(classificationOperator);
				
				operatorGraph.addVertex(classificationOperator);
				
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
						
						operatorGraph.addVertex(projectOperator);
						
						operatorGraph.addEdge(new DataFlow(previousOperator, projectOperator));
						
						projectOperator.inputSchema = previousOperator.outputSchema.copy();
						projectOperator.inputRate = previousOperator.outputRate;
						projectOperator.inputName = previousOperator.outputName;
						
						projectOperator.outputRate = projectOperator.inputRate;
					}
					
					operatorGraph.addEdge(new DataFlow(projectOperator, classificationOperator));
					
					classificationOperator.inputSchema = projectOperator.outputSchema.copy();
					classificationOperator.inputRate = projectOperator.outputRate;
					classificationOperator.inputName = projectOperator.outputName;
					
					classificationOperator.outputRate = classificationOperator.inputRate;
				}
				else
				{
					//ProjectNotNeeded
					operatorGraph.addEdge(new DataFlow(previousOperator, classificationOperator));
					
					classificationOperator.inputSchema = previousOperator.outputSchema.copy();
					classificationOperator.inputRate = previousOperator.outputRate;
					classificationOperator.inputName = previousOperator.outputName;
					
					classificationOperator.outputRate = classificationOperator.inputRate;
				}
			}
		}
		
		for(int index = 0; index < projectOperators.size(); index++)
		{
			projectOperator = projectOperators.get(index);
			
			projectOperator.group -= 1;
		}
		
		previousOperators.clear();
		previousOperators.addAll(classificationOperators);
		previousOperator = classificationOperators.get(0);
		
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
			operatorGraph.getNextGroup();
			
			for(int index = 0; index < previousOperators.size(); index++)
			{
				previousOperator = previousOperators.get(index);
				
				outlierRemovingOperator = OperatorGenerator.generateOutlierRemovingOperator(previousOperator.models.get(0).getModel_title());
				
				outlierRemovingOperator.models.add(previousOperator.models.get(0));
				
				outlierRemovingOperators.add(outlierRemovingOperator);
				
				operatorGraph.addVertex(outlierRemovingOperator);
				
				operatorGraph.addEdge(new DataFlow(previousOperator, outlierRemovingOperator));
				
				outlierRemovingOperator.inputSchema = previousOperator.outputSchema.copy();
				outlierRemovingOperator.inputRate = previousOperator.outputRate;
				outlierRemovingOperator.inputName = previousOperator.outputName;
				
				outlierRemovingOperator.outputSchema = outlierRemovingOperator.inputSchema.copy();
				outlierRemovingOperator.outputRate = outlierRemovingOperator.inputRate;
			}
			
			previousOperators.clear();
			previousOperators.addAll(outlierRemovingOperators);
			previousOperator = outlierRemovingOperators.get(0);
			
			System.out.println("Generated " + outlierRemovingOperators.size() + " OutlierRemovingOperators");
			
			//8 - ChangedetectOperator
			operatorGraph.getNextGroup();
			
			for(int index = 0; index < previousOperators.size(); index++)
			{
				previousOperator = previousOperators.get(index);
				
				changedetectOperator = OperatorGenerator.generateChangedetectOperator(previousOperator.models.get(0).getModel_title());
				
				changedetectOperator.models.add(previousOperator.models.get(0));
				
				changedetectOperators.add(changedetectOperator);
				
				operatorGraph.addVertex(changedetectOperator);
				
				outputStreams.add(changedetectOperator.outputName);
				
				operatorGraph.addEdge(new DataFlow(previousOperator, changedetectOperator));
				
				changedetectOperator.inputSchema = previousOperator.outputSchema.copy();
				changedetectOperator.inputRate = previousOperator.outputRate;
				changedetectOperator.inputName = previousOperator.outputName;
				
				changedetectOperator.outputSchema = changedetectOperator.inputSchema.copy();
				changedetectOperator.outputRate = changedetectOperator.inputRate * 0.5;
			}
			
			previousOperators.clear();
			previousOperators.addAll(changedetectOperators);
			previousOperator = changedetectOperators.get(0);
			
			System.out.println("Generated " + changedetectOperators.size() + " ChangedetectOperators");
		}
		
		//8.5 - MergeOperator
		mergeOperator = null;
		
		if(merge)
		{
			if(previousOperators.size() > 1)
			{
				operatorGraph.getNextGroup();
				
				mergeOperator = OperatorGenerator.generateMergeOperator(outputStreams);
				
				operatorGraph.addVertex(mergeOperator);
				
				outputRateSum = 0.0d;
				
				for(int index = 0; index < previousOperators.size(); index++)
				{
					previousOperator = previousOperators.get(index);
					
					operatorGraph.addEdge(new DataFlow(previousOperator, mergeOperator));
					
					outputRateSum += previousOperator.outputRate;
				}
				
				mergeOperator.inputSchema = previousOperator.outputSchema.copy();
				mergeOperator.inputRate = outputRateSum;
				mergeOperator.inputName = null;
	
				mergeOperator.outputSchema = mergeOperator.inputSchema.copy();
				mergeOperator.outputRate = mergeOperator.inputRate;
				
				previousOperators.clear();
				previousOperators.add(mergeOperator);
				previousOperator = mergeOperator;
			}
			
			System.out.println("Generated " + (mergeOperator==null?"0":"1") + " MergeOperators");
		}
		
		//9 - DatabasesinkOperator
		operatorGraph.getNextGroup();
		
		DatabasesinkOperator databasesinkOperator = OperatorGenerator.generateDatabasesinkOperator("prediction");
		
		operatorGraph.addVertex(databasesinkOperator);
		
		outputRateSum = 0.0d;
		
		for(int index = 0; index < previousOperators.size(); index++)
		{
			previousOperator = previousOperators.get(index);
			
			operatorGraph.addEdge(new DataFlow(previousOperator, databasesinkOperator));
			
			outputRateSum += previousOperator.outputRate;
		}
		
		databasesinkOperator.inputSchema = previousOperator.outputSchema.copy();
		databasesinkOperator.inputRate = outputRateSum;
		databasesinkOperator.inputName = previousOperators.size()==1?previousOperator.outputName:"undefined";
		
		databasesinkOperator.outputSchema = databasesinkOperator.inputSchema.copy();
		databasesinkOperator.outputRate = databasesinkOperator.inputRate;
		
		System.out.println("Generated 1 DatabasesinkOperator");
		
		operatorGraph.setControlFlowDatarates();
		
		operatorGraph.setLabels();
		
		endTimestamp = System.currentTimeMillis();
		
		System.out.println("...Generation of Operator Graph finished after " + Util.formatTimestamp(endTimestamp - startTimestamp) + "\n");
		
		return operatorGraph;
	}
}