package it.uniroma3.radeon.functions.accessories;

import java.lang.reflect.Method;

import it.uniroma3.radeon.data.TweetData;

public class EqualsCondition extends TweetCondition {
	
	private static final long serialVersionUID = 1L;

	public EqualsCondition(String attribute, Object condition) {
		super(attribute, condition);
	}
	
	public Boolean verify(TweetData td) {
		String[] accessList = this.getAccessList();
		
		try {
			String getterName = "get" + accessList[0];
			Method getterMethod = td.getClass().getMethod(getterName);
			Object currentAccess = getterMethod.invoke(td);
			Class<?> returnType = getterMethod.getReturnType();
			for (int i = 1; i < accessList.length; i +=1) {
				getterName = "get" + accessList[i];
				getterMethod = returnType.cast(currentAccess).getClass().getMethod(getterName);
				currentAccess = getterMethod.invoke(returnType.cast(currentAccess));
				returnType = getterMethod.getReturnType();
			}
			return currentAccess.equals(this.getCondition());
		}
		catch (NoSuchMethodException e) {
			e.printStackTrace();
			return false;
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}

}
