package com.ybigta.hadoopproject201803;

import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LevelMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

   public String[] kindOfLevel = {"beginner","easy","medium", "hard", "challenge"}; // level 종류
   private Text word;

   static String arrayJoin(String glue, String array[]) {
      String result = "";

      for (int i = 0; i < array.length; i++) {

         result += array[i];
         if (i < array.length - 1) result += glue;
      }
      return result;
   }

   

   @Override
   public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {
      // 위에서 Text level, Text[] tags 형태로 주면
      // tags에서 일치허는거 있는지 찾고 있으면 얼마나 다른지
      // default values
//      String level = key.toString(); //TODO Text to String.....!
//      String[] tags = value.toString().split(""); //{"df1","df2","default"};
      // 끝
      
      String[] row = value.toString().split(",");
      String level = row[0];
      String tag = null;
      String[] rawTag = new String[row.length-1]; 
      System.arraycopy(row, 1, rawTag, 0, row.length-1); // level정보만 빼고 나머지 배열 복사
      tag = arrayJoin(",", rawTag); //"['a', 'b', 'c']" 이런 형태가 됨
      tag = tag.replaceAll("\"", ""); // 쌍따옴표 제거
      if(tag.equals("[]")|| tag.contains("</td>")) tag = null; //없거나 이상한 값이 있는 경우 jump
      else {
         tag=tag.substring(1, tag.length()-1); //[ ] 제거      
         rawTag = tag.split(","); 
         boolean levelFound = false; // level 값이 tag안에 속해있는지 여부 판별
         for(int i=0; i<rawTag.length; i++){ // tag 안에 값들에 대한 for문 
            rawTag[i] = rawTag[i].replace("\'", "");
            for(int j=0; j<kindOfLevel.length; j++){ // 실제 level 종류와 비교를 위한 for문 
               if (rawTag[i].contains(kindOfLevel[j])){
                  levelFound = true;
                  break;
               }
            }
   
            if (levelFound){
               tag = rawTag[i];
               break; 
            }
         }
         if(!levelFound) tag = null;
      }

          Integer lev1, lev2;
          Double diff;
          String txt = tag;
          if(txt!=null) {
        	  if(txt.equals("beginner")) lev1 = 1;
	          else if(txt.equals("easy")) lev1 = 3;
	          else if(txt.equals("easy-medium")) lev1 = 4;
	          else if(txt.equals("medium")) lev1 = 5;
	          else if(txt.equals("medium-hard")) lev1 = 6;
	          else if(txt.equals("hard")) lev1 = 7;
	          else if(txt.equals("challenging")) lev1 = 9;
	          else /*없음*/ lev1 = null;
          }else
        	  lev1 = null;

          
           if(level!=null) {
               if(level.equals("beginner")) lev2 = 1;
               else if(level.equals("easy")) lev2 = 3;
               else if(level.equals("medium")) lev2 = 5;
               else if(level.equals("hard")) lev2 = 7;
               else if(level.equals("challenging")) lev2 = 9;
               else lev2 = null;
           }else
        	   lev2=null;


      if(lev1!=null && lev2!=null) {
    	  diff = (double) lev2 - lev1;
    	  
    	  /* diff가 존재하는 경우 */
          DoubleWritable diffWr = new DoubleWritable(diff);
          
          word = new Text(level);
          context.write(word, diffWr);
       }
      }

}