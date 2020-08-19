import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.alibaba.fastjson.JSONObject;

public class Teat {
    private int count = 10;
    public String get_str(String str){
        String pre = "_"+"aa";
        String s = str.trim().isEmpty() || str == null ? "test" : str;
        return s.trim()+pre;
    }
    public String get_count(){
        String json_count = String.format("{\"segment_row_count\": %s}",count);
        return json_count;
    }
    public Object[][] get_object(){
        Object[][] objects;
        String name = "aaa";
        int num = 2;
        objects = new Object[][]{{num, name}};
        return objects;
    }
    public static void main(String[] args) {
//        Integer a = 100;
//        String json_segment_row_count = String.format("{\"segment_row_count\": %s, \"auto_id\": %s}",100, false);
////        String str1 = "params:{\"dim:128}";
//        System.out.println(json_segment_row_count);
//        JSONObject jo = JSONObject.parseObject(json_segment_row_count);
//        int para = jo.getInteger("segment_row_count");
//        System.out.println(jo.getString("auto_id"));
//        System.out.println(para);
//        int count = jo.getInteger("segment_row_count");
//        System.out.println(count);
        List<Map<Integer,String>> list = new ArrayList<Map<Integer, String>>();
        Map<Integer,String> map = new HashMap<Integer, String>();
        map.put(1,"a");
        map.put(2,"b");
        Map<Integer,String> map1 = new HashMap<Integer, String>();
        map1.put(3,"aa");
        map1.put(4,"bb");
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("dim", 128);
        map1.put(5, jsonObject.toString());
        list.add(map);
        list.add(map1);
        String param = list.get(list.size()-1).get(5);
        System.out.println("param "+param);
        JSONObject jo = JSONObject.parseObject(param);
        System.out.println(jo.getInteger("dim"));

    }
}
