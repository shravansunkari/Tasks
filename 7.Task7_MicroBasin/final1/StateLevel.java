protected static final Map<String, Map<String, ILocationStructureInfo>> locToStructureCountMap = new HashMap<>();
static {

Map<String, ILocationStructureInfo> structTypeToMasterDataMap = null;

structTypeToMasterDataMap = new HashMap<>();
locToStructureCountMap.put("Andhra Pradesh", structTypeToMasterDataMap);
structTypeToMasterDataMap.put(CHECKDAM, new LocationStructureInfo(36942, 13.044220199999328));
structTypeToMasterDataMap.put(FARM_POND, new LocationStructureInfo(223573, 1.5873682999998058));
structTypeToMasterDataMap.put(PERCULATION_TANK, new LocationStructureInfo(9179, 3.240187000000113));
structTypeToMasterDataMap.put(MI_TANK, new LocationStructureInfo(37014, 178.99139641291842));
structTypeToMasterDataMap.put(OTHERS, new LocationStructureInfo(10797, 3.811341000000093));
structTypeToMasterDataMap.put(CHECK_DAM_PROPOSED, new LocationStructureInfo(20047, 7.078595699999999));
structTypeToMasterDataMap.put(TOTAL, new LocationStructureInfo(317505, 200.674512913));

}