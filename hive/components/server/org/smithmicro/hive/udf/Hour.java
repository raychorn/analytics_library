/* 
 * File Name: Hour.java
 * Purpose: A Hive UDF that returns the full Hour with initial 0, etc.
 */

package com.smithmicro.hive.udf;

import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class Hour extends UDF {
  public Text evaluate(final Text s) { 
	if (s == null) { return null; }
	try {
		String[] formatStrings = {"yyyy-MM-dd HH:mm:ss", "HH:mm:ss"};
		Date d = null;
		for (String formatString : formatStrings)
		{
			try
			{
			    d = new SimpleDateFormat(formatString).parse(s.toString());
			    break;
			}
			catch (ParseException e) {}
		}
		SimpleDateFormat sdf2 = new SimpleDateFormat("HH");
           	return new Text(sdf2.format(d));	
	} catch (NullPointerException e) {
		e.printStackTrace();
		return null;
	}
  }
}
