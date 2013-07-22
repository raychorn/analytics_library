/* 
 * File Name: DayOfWeek.java
 * Purpose: A Hive UDF that returns the day of the week, ex: Monday, Tuesday, etc.
 */

package com.smithmicro.hive.udf;

import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class Day extends UDF {
  public Text evaluate(final Text s) { 
	if (s == null) { return null; }
       	try {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	  	//Date date = (Date)sdf.parse("2011-04-14 03:14:15");
	  	Date date = (Date)sdf.parse(s.toString());
		// System.out.println("date = " + date.toString());
		SimpleDateFormat sdf2 = new SimpleDateFormat("dd");
           	return new Text(sdf2.format(date));	
	} catch (ParseException e) {
		e.printStackTrace();
		return null;
	}
  }
}
