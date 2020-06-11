package cn.piflow.bundle.util;

import com.mongodb.BasicDBObject;

import java.lang.reflect.Field;

/**
 * Created by computer on 2019/6/12.
 */
public class Until {
    public static BasicDBObject getDBObjFromJavaBean(Object beanObj) {
        BasicDBObject doc = null;
        try {
            doc = new BasicDBObject();
            Field[] methodList = beanObj.getClass().getDeclaredFields();
            for (int i = 0; i < methodList.length; i++) {
                Field field = methodList[i];
                field.setAccessible(true);
                Object value = field.get(beanObj);
                if (value != null) {
                    doc.put(field.getName(), field.get(beanObj));
                } else {
                    doc.put(field.getName(), field.get(beanObj));
                }
            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            doc = null;
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            doc = null;
        }
        return doc;
    }
}
