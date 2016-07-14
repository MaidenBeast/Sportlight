package it.uniroma3.radeon.sa.utils;

import java.lang.reflect.Method;
import java.util.Arrays;

public class ObjectNavigator {
	
	private Object instance;
	private Class<?> instanceType;
	
	public ObjectNavigator(Object instance, Class<?> type) {
		this.instance = instance;
		this.instanceType = type;
	}
	
	public Object retrieveField(String field) throws NullPointerException {
		Object res = this.accessField(field);
		if (res != null) {
			return res;
		}
		else {
			throw new NullPointerException();
		}
	}

	public Object retrieveField(String[] fieldPath) {
		String currentField = fieldPath[0];
		if (fieldPath.length == 1) {
			return this.retrieveField(currentField);
		}
		else {
			try {
				Method getterMethod = this.getGetter(currentField);
				Class<?> methodRetType = getterMethod.getReturnType();
				
				Object returned = this.retrieveField(currentField);
				
				ObjectNavigator nextNav = new ObjectNavigator(returned, methodRetType);
				returned = nextNav.retrieveField(Arrays.copyOfRange(fieldPath, 1, fieldPath.length));
				return returned;
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
	
	private Object accessField(String field) {
		try {
			Method getterMethod = this.getGetter(field);
			Object returned = getterMethod.invoke(this.instanceType.cast(this.instance));
			return returned;
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
	
	private Method getGetter(String field) throws NoSuchMethodException, Exception {
		Character firstLetter = Character.toUpperCase(field.charAt(0));
		String fieldName = firstLetter + field.substring(1);
		String getterName = "get" + fieldName;
		
		Method getterMethod = this.instanceType.getMethod(getterName);
		return getterMethod;
	}
}