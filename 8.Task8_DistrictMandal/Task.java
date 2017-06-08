import java.util.*;
public class Task{

	//Map<VillageFullName, Map<GridID, Area>>
	public double computeAvailableSMPercentage(String villageFullName, 
		Map<Long, Double> gridData){

		double available_soilmoisture = 0.0;

		for(Long gridValue : gridData.keySet()){

			available_soilmoisture += (gridValue * gridData.get(gridValue));
		}

		return available_soilmoisture;

	}

	public static void main(String[] args){
		
	}
}