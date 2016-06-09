package it.uniroma3.radeon.sportlight.utils;

import java.lang.reflect.Method;
import java.util.Arrays;

public class ObjectNavigator {
	
	private Object instance;
	private Class<?> instanceType;
	
	public ObjectNavigator(Object instance, Class<?> type) {
		this.instance = instance;
		this.instanceType = type;
	}
	
	public Object retrieveField(String[] fieldPath) {
		String getterName = "get" + fieldPath[0];
		try {
			Method getterMethod = this.instanceType.getMethod(getterName);
			Object returned = getterMethod.invoke(this.instanceType.cast(this.instance));
			if (fieldPath.length == 1) {
				return returned;
			}
			else {
				Class<?> getterReturnType = getterMethod.getReturnType();
				ObjectNavigator nextNav = new ObjectNavigator(returned, getterReturnType);
				returned = nextNav.retrieveField(Arrays.copyOfRange(fieldPath, 1, fieldPath.length));
				return returned;
			}
		}
		catch (NoSuchMethodException e) {
			e.printStackTrace();
			return null;
		}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
