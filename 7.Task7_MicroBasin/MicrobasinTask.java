/**
     * Map From LocationFullName to StructureType to LocationStructureInfo
     * LocationFullName is in (D##M##V...) Format
     */
    protected static final Map<String, Map<String, ILocationStructureInfo>> locToStructureCountMap = new HashMap<>();
   
    static {   
       
        Map<String, ILocationStructureInfo> structTypeToCount = new HashMap<>();
        
        locToStructureCountMap.put("XXXX", structTypeToCount);
        structTypeToCount.put(CHECKDAM, new LocationStructureInfo(10, 10));
        structTypeToCount.put(FARM_POND, new LocationStructureInfo(10, 10));
        structTypeToCount.put(CHECK_WALL, new LocationStructureInfo(10, 10));
        structTypeToCount.put(PERCULATION_TANK, new LocationStructureInfo(10, 10));
        structTypeToCount.put(MINI_PERCULATION_TANK, new LocationStructureInfo(10, 10));
    }