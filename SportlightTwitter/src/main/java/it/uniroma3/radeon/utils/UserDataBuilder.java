package it.uniroma3.radeon.utils;

import java.io.Serializable;

import it.uniroma3.radeon.data.UserData;
import twitter4j.User;

public class UserDataBuilder implements Serializable {
	
	private static final long serialVersionUID = 1L;

	public UserData buildFromUser(User user) {
		UserData udata = new UserData();
		
		udata.setName(user.getName());
		
		return udata;
	}
}
