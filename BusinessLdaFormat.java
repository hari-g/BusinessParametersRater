/* BusinessLdaFormat.java
 * Business Analyzer cloud Final Project
 * Hari Krishna Gajarla and Venkat
 * Group 19
 */



import com.fasterxml.jackson.annotation.*;
@JsonIgnoreProperties(ignoreUnknown = true)
public class BusinessLdaFormat{
   	private String business_id;
   	private Number business_rating;
   	private Number review_rating;
   	private String text;
   	private String type;

 	public String getBusiness_id(){
		return this.business_id;
	}
	public void setBusiness_id(String business_id){
		this.business_id = business_id;
	}
 	public Number getBusiness_rating(){
		return this.business_rating;
	}
	public void setBusiness_rating(Number business_rating){
		this.business_rating = business_rating;
	}
 	public Number getReview_rating(){
		return this.review_rating;
	}
	public void setReview_rating(Number review_rating){
		this.review_rating = review_rating;
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