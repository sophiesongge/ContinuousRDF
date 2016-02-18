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
		// Tricky part is that Task Ids start from 2. e.g., for 3 tasks [2 3 4] and for for 7 tasks [2 3 4 5 6 7 8]
		System.out.println("Targeted Tasks are:" + targetTasks.toString());
		
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		
		List<Integer> boltIds = new ArrayList();
		
        if(values.size()>0){
            String predicate = values.get(1).toString();
            
            if(predicate.equalsIgnoreCase("work")) {
            	//int i = (int) Math.random()*5;
            	//if(i == 0){
                	boltIds.add(2);            		
            	/*}else if(i == 1){
                	boltIds.add(3);            		
            	}else if(i == 2){
                	boltIds.add(4);            		
            	}else if(i == 3){
                	boltIds.add(5);     
	        	}else{//i == 4
	            	boltIds.add(6);            		
	        	}*/
            }
            else if(predicate.equalsIgnoreCase("paper")) {
            	boltIds.add(3);
            }
            else if(predicate.equalsIgnoreCase("diplome")) {
            	boltIds.add(4);
            }
            else
            	boltIds.add(0);
            	//System.out.println("Error, con't identify predicate");
        }
        
        return boltIds;
	}

}
