/* BusinessFormater.java
 * Business Analyzer cloud Final Project
 * Hari Krishna Gajarla and Venkat
 * Group 19
 */

//import java.util.List;

import com.fasterxml.jackson.annotation.*;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BusinessFormater{
   	private String business_id;
   	private Number stars;
   	private String text;
   	private String type;

 	public String getBusiness_id(){
		return this.business_id;
	}
	public void setBusiness_id(String business_id){
		this.business_id = business_id;
	}
 	public Number getStars(){
		return this.stars;
	}
	public void setStars(Number stars){
		this.stars = stars;
	}
 	public String getText(){
		return this.text;
	}
	public void setText(String text){
		this.text = text;
	}
 	public String getType(){
		return this.type;
	}
	public void setType(String type){
		this.type = type;
	}
}
