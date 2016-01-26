package storm.grouping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class PredicateGrouping implements CustomStreamGrouping,Serializable {

    int numTasks = 3;
    
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		// TODO Auto-generated method stub
		
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		
		List<Integer> boltIds = new ArrayList();
		
        if(values.size()>0){
            String predicate = values.get(1).toString();
            
            if(predicate.equalsIgnoreCase("work")) {
            	boltIds.add(0);
            }
            else if(predicate.equalsIgnoreCase("paper")) {
            	boltIds.add(3);
            }
            else if(predicate.equalsIgnoreCase("diplome")) {
            	boltIds.add(2);
            }
            else
            	boltIds.add(0);
            	//System.out.println("Error, con't identify predicate");
            /*
            if(predicate.isEmpty())
            {
                boltIds.add(0);
                System.out.println("checkpoint id is"+boltIds.toString());
            }
            else
                boltIds.add(predicate.charAt(0) % numTasks);*/
        }
        
        return boltIds;
	}

}
