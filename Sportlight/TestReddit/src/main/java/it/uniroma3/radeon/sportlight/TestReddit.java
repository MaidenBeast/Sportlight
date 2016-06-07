package it.uniroma3.radeon.sportlight;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Calendar;
import java.util.Date;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestReddit {
	private static final String REDDIT_URL_TEMPLATE = "https://www.reddit.com/r/Euro2016/.json?sort=new&raw_json=1";
	
	public void bootstrap() {
		Calendar cal = Calendar.getInstance();
		Date today = cal.getTime();
		cal.add(Calendar.YEAR, -1); //prendo la data dell'anno precedente
		long prevYearTimeStamp = cal.getTime().getTime();
		
		boolean toIterate = true;
		
		String after_param = "";
		URL url = null;
		do {
			try {
				url = new URL(REDDIT_URL_TEMPLATE.concat(after_param));
				URLConnection conn = url.openConnection();
				
				//workaround per l'errore HTTP 429 (Too Many Requests)
				conn.setRequestProperty("User-Agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:46.0) Gecko/20100101 Firefox/46.0");
				
				ObjectMapper mapper = new ObjectMapper();
				
				System.out.println("\nGetting Reddit data from "+url.toURI());
				
				JsonNode jsonRoot = mapper.readTree(conn.getInputStream());
				JsonNode jsonData = jsonRoot.get("data");
				JsonNode jsonChildren = jsonData.get("children");
				
				after_param = "&after=".concat(jsonData.get("after").asText());
				
				for (JsonNode jsonChild : jsonChildren) { 
					JsonNode jsonChildData = jsonChild.get("data");
					long createTime = jsonChildData.get("created").asLong()*1000;
					
					//se il post è stato pubblicato un'anno fa, allora blocca entrambi i cicli
					if (createTime < prevYearTimeStamp) {
						toIterate = false;
						break;
					}
					System.out.println(jsonChildData);
				}
				
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				Thread.sleep(1000); //attendi per un secondo (per evitare eventuali blocchi)
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} //attendi 10 secondi
		} while (toIterate);
	}
}
