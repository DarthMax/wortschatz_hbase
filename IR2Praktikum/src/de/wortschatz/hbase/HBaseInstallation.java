package de.wortschatz.hbase;


import javax.json.*;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Marcel Kisilowski on 02.02.15.
 */
public class HBaseInstallation {

    public static boolean createTables(String jsonfile) {
        HBaseCRUDer cruder = new HBaseCRUDer(HBaseConnector.getConnection());
        cruder.createTables(jsonToMap(jsonfile));
        return true;
    }
    public static Map<String,List<String>> jsonToMap(String jsonfile) {
        Map<String,List<String>> resultMap = new HashMap<String,List<String>>();
        List<String> famList;
        String tableName;
        try {
            JsonReader reader = Json.createReader(new FileReader(jsonfile));
            JsonObject obj = reader.readObject();
            JsonArray tables = obj.getJsonArray("tables");
            JsonArray colFamilies;
            for (JsonObject table : tables.getValuesAs(JsonObject.class)) {
                famList = new ArrayList();
                tableName = table.getString("tableName");
                colFamilies = table.getJsonArray("columnFamilies");
                for(JsonObject colFamily: colFamilies.getValuesAs(JsonObject.class)){
                    famList.add(colFamily.getString("familyName"));
                }
                resultMap.put(tableName,famList);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return new HashMap<>();
        }
        return resultMap;
    }

}
