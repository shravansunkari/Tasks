package controllers.com.vassarlabs.play.controllers;

import org.springframework.stereotype.Controller;

import play.libs.Json;
import play.mvc.Result;

import com.fasterxml.jackson.databind.JsonNode;
import com.vassarlabs.common.context.impl.AppContext;
import com.vassarlabs.common.exceptions.ObjectReadException;
import com.vassarlabs.common.object.mapper.ObjectWrapMapper;
import com.vassarlabs.prapp.main.MiTankCrudController;

@Controller
public class MITankController extends BasicController{

	private MiTankCrudController miCrudController = null;
	
	public synchronized MiTankCrudController getMiTankCrudController() {
		if (miCrudController != null) {
			return miCrudController;
		}
		miCrudController = (MiTankCrudController) AppContext.getApplicationContext().getBean(MiTankCrudController.class);
		return miCrudController;
	}
	
	
	public Result getMiTankTableData(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().getMiTankTableData(requestString);
			
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	
	public Result getMiTankDetails(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().getMiTankDetails(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	public Result getTransactionData(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().getTransactionData(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	
	public Result getHeatMapImage(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().getHeatMapImage(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	
	
	public Result getMainDashboardMapForCapacity(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		String jsonInString = null;
		jsonInString = getMiTankCrudController().getMainDashboardMapForCapacity();
		return ok(Json.toJson(jsonInString));
	}
	
	
	public Result getMainDashboardMapForPercentage(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		String jsonInString = null;
		jsonInString = getMiTankCrudController().getMainDashboardMapForPercentage();
		return ok(Json.toJson(jsonInString));
	}
	
	
	
	public Result getAllMiTanksOfUser(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().getAllMiTanksOfUser(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	public Result notMiTankService(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().notMiTankService(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	
	public Result duplicateOrDeleteTanks(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().duplicateOrDeleteTanks(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	public Result getUsersDesignations(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().getUsersDesignations(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	
	public Result deleteProjects(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().deleteProjects(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	public Result getHierarchyOfLocations(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().getHierarchyOfLocations(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	
	public Result insertOrEditAProject(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().insertOrEditAProject(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	public Result insertOrEditAUser(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().insertOrEditAUser(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	
	public Result getMiTankBasinLevelData(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().getMiTankBasinLevelData(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	public Result getStatusReports(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().getStatusReports(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	
	public Result getUsersInaHierarchy(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().getUsersInaHierarchy(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	public Result getAllUsers(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().getAllUsers(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	
	public Result getTanksListInaHierarchy(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().getTanksListInaHierarchy(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	public Result getReports(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().getReports(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	
	public Result searchMITank(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().searchMITank(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	public Result assignOrReassignProjects(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().assignOrReassignProjects(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
	
	
	public Result fileDownload(){
		ObjectWrapMapper mapper = new ObjectWrapMapper();
		JsonNode jsonNode = request().body().asJson();
		String requestString,jsonInString = null;
		try {
			requestString = mapper.writeValueAsString(jsonNode);
			jsonInString = getMiTankCrudController().fileDownload(requestString);
		} catch (ObjectReadException e) {
			e.printStackTrace();
		}
		return ok(Json.toJson(jsonInString));
	}
	
}
