package bench;

import java.util.Properties;

/**
 * Created by gomes on 06/05/15.
 */
public class Bundle extends Properties {


    int getInteger(String key){
        return Integer.parseInt(this.getProperty(key));
    }


}
