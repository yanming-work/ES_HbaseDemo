package com.test.core.result;
import java.lang.annotation.ElementType;  
import java.lang.annotation.Retention;  
import java.lang.annotation.RetentionPolicy;  
import java.lang.annotation.Target;  
  
/**
 * 
* @ClassName: ColumnConfig 
* @Description: 用于配置实体类字段说明信息 
* @author YanMing
* @date 2017年3月29日 上午11:52:28 
*
 */
@Target(ElementType.FIELD)  
@Retention(RetentionPolicy.RUNTIME)  
public @interface ColumnConfig {  
    /** 
     * 字段的中文名 
     * @return 
     */  
    String description() default "";  
    /**
     * 
    * @Title: dbColumnName 
    * @Description: 字段表名称 
    * @param @return    设定文件 
    * @return String    返回类型 
    * @throws
     */
    String dbColumnName() default "";
}  