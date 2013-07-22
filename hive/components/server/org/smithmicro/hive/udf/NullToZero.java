/* 
 * File Name: NullToZero.java
 * Purpose: A Hive UDF that returns 0 when the value at the Hive is null. This will solve the issue of generating results as null values when adding two hive columns and one of them is null  
 */

package com.smithmicro.hive.udf;

import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class NullToZero extends UDF {
  public Text evaluate(final Text s) { 
	if ((s == null) || (s.equals(""))) { 
		return new Text("0"); 
 	}
	return s; 
  }
}
