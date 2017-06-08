import java.util.*;
public class test{

	public static Map<String, Map<String, String>> total_map = new HashMap<>();
	public static void main(String[] args){
		Map<String, String> map1 = new HashMap<String, String>();
		total_map.put("shravan",map1);
		map1.put("a","b");
		map1.put("a1","b1");

		map1 = new HashMap<String, String>();
		total_map.put("chintu",map1);
		map1.put("c","d");
		map1.put("c1","d1");

		for(String map : total_map.keySet()){
			System.out.print("Name ="+map);
			for(String m: total_map.get(map).keySet()){
				System.out.println(" => key= "+m+" value="+total_map.get(map).get(m));
			}
		}
	}
}