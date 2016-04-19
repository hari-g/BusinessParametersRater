/* BusinessAttrib.java
 * Business Analyzer cloud Final Project
 * Hari Krishna Gajarla and Venkat
 * Group 19
 */

import java.util.List;

import com.fasterxml.jackson.annotation.*;
@JsonIgnoreProperties(ignoreUnknown = true)
public class BusinessAttrib{
   	private String business_id;
   	private List<String> categories;
   	private Number stars;
   	private String type;

 	public String getBusiness_id(){
		return this.business_id;
	}
	public void setBusiness_id(String business_id){
		this.business_id = business_id;
	}
 	public List<String> getCategories(){
		return this.categories;
	}
	public void setCategories(List<String> categories){
		this.categories = categories;
	}
 	public Number getStars(){
		return this.stars;
	}
	public void setStars(Number stars){
		this.stars = stars;
	}
 	public String getType(){
		return this.type;
	}
	public void setType(String type){
		this.type = type;
	}
}
