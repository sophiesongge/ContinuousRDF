package storm.topology;

public class API {
	
	public String[] singleVarJoin(String var){		
		return multiVarJoin(var, "any", "any");
	}
	
	public String[] doubleVarJoin(String var1,String var2){		
		return multiVarJoin(var1, var2, "any");		
	}
	
	public String[] multiVarJoin(String var1, String var2, String var3){
		String[] returnval = new String[]{};
		//todo
		return returnval;
	}
}
