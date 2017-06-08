'use strict'

module.exports = function(java){

  var testInstance = java.newInstanceSync("com.vassarlabs.prapp.main.MiTankCrudController");
  var service = {
    getMiTankTableData : getMiTankTableData,
    getMiTankDesc: getMiTankDesc,
    getTransactionData : getTransactionData,
    getMapDataForCapacity : getMapDataForCapacity,
    getMapDataForPercentage : getMapDataForPercentage,
    getAllMitanksofUser : getAllMitanksofUser,
    submitNotMitank  : submitNotMitank,
    getLocationHierarchyData : getLocationHierarchyData,
    getImageById : getImageById,
    getSearchData : getSearchData,
    getReports : getReports,
    insertOrEditAProject : insertOrEditAProject,
    insertOrEditAUser : insertOrEditAUser,
    assignOrReassignProjects : assignOrReassignProjects,
    deleteProjects : deleteProjects,
    getAdminReports : getAdminReports,
    getAllUsers : getAllUsers,
    getUsersDesignations : getUsersDesignations,
    getTanksListInaHierarchy : getTanksListInaHierarchy,
    getUsersInaHierarchy : getUsersInaHierarchy,
    getBasinLevelData : getBasinLevelData
  };

  return service;

  /**
   * all function should be written here
   *
   */
  function insertOrEditAProject(req,res){
    var data = {};
    var param = req.body;
    java.callMethod(testInstance,"insertOrEditAProject",JSON.stringify(param),function(err, result){
    data = result;
    res.send(JSON.parse(data));
    });
  }
  function insertOrEditAUser(req,res){
    var data = {};

    var param = req.body;
    java.callMethod(testInstance,"insertOrEditAUser",JSON.stringify(param),function(err, result){
      data = result;

      res.send(JSON.parse(data));
    });

  }
  function getTanksListInaHierarchy(req,res){
    var data = {};
    var param = req.body;
    java.callMethod(testInstance,"getTanksListInaHierarchy",JSON.stringify(param),function(err, result){
      data = result;
      res.send(JSON.parse(data));
    });

  }
  function getUsersInaHierarchy(req,res){
    var data = {};
    var param = req.body;
    java.callMethod(testInstance,"getUsersInaHierarchy",JSON.stringify(param),function(err, result){
      data = result;
      res.send(JSON.parse(data));
    });

  }
  function assignOrReassignProjects(req,res){
    var data = {};
    var param = req.body;
    //console.log("Req :: "+JSON.stringify(param));
    java.callMethod(testInstance,"assignOrReassignProjects",JSON.stringify(param),function(err, result){
      data = result;
      res.send(JSON.parse(data));
    });

  }

  function getUsersDesignations(req,res){
    var data = {};
    var param = req.body;

    java.callMethod(testInstance,"getUsersDesignations",JSON.stringify(param),function(err, result){
      data = result;
      res.send(JSON.parse(data));
    });

  }

  function deleteProjects(req,res){
    var data = {};
    var param = req.body;
    java.callMethod(testInstance,"deleteProjects",JSON.stringify(param),function(err, result){
      data = result;
      res.send(JSON.parse(data));
    });

  }
  function getAllUsers(req,res){
    var data = {};
    var param = req.body;
    java.callMethod(testInstance,"getAllUsers",JSON.stringify(param),function(err, result){
      data = result;
      res.send(JSON.parse(data));
    });

  }
  function getMiTankTableData(req,res){
    var data = {};
    var param = req.body;
    java.callMethod(testInstance,"getMiTankTableData",JSON.stringify(param),function(err, result){
      data = result;
      res.send(JSON.parse(data));
    });
  }

  function getAdminReports(req,res){
    var data = {};
    var param = req.body;
    java.callMethod(testInstance,"getAdminReports",JSON.stringify(param),function(err, result){
        data = result;
        res.send(JSON.parse(data));
    });
  }

  function getAllMitanksofUser(req,res){
    var param = req.body;
    // var testInstance = java.newInstanceSync("com.vassarlabs.prapp.main.MiTankCrudController");
    // var result = java.callMethodSync(testInstance,"getAllMiTanksOfUser",JSON.stringify(param));
    // res.send(result);

    java.callMethod(testInstance,"getAllMiTanksOfUser",JSON.stringify(param),function(err, result){
        res.send(result);
    });
  }
  function submitNotMitank(req,res){
    var param = req.body;
    // var testInstance = java.newInstanceSync("com.vassarlabs.prapp.main.MiTankCrudController");
    // var result = java.callMethodSync(testInstance,"notMiTankService",JSON.stringify(param));
    // res.send(result);

    java.callMethod(testInstance,"notMiTankService",JSON.stringify(param),function(err, result){
        res.send(result);
    });
  }
/**
  function submitDuplicateTank(req,res){
    var param = req.body;
    // var testInstance = java.newInstanceSync("com.vassarlabs.prapp.main.MiTankCrudController");
    // var result = java.callMethodSync(testInstance,"duplicateMiTankService",JSON.stringify(param));
    // res.send(result);
    java.callMethod(testInstance,"duplicateOrdeleteProjects",JSON.stringify(param),function(err, result){
        res.send(result);
    });
  }
*/
  function getLocationHierarchyData(req,res){
    var param = req.body;
    // var testInstance = java.newInstanceSync("com.vassarlabs.prapp.main.MiTankCrudController");
    // var result = java.callMethodSync(testInstance,"getHierarchyOfLocations",JSON.stringify(param));
    // res.send(result);
    java.callMethod(testInstance,"getHierarchyOfLocations",JSON.stringify(param),function(err, result){
        res.send(result);
    });

  }

/** function submitNewTank(req,res){
    var param = req.body;
    // var testInstance = java.newInstanceSync("com.vassarlabs.prapp.main.MiTankCrudController");
    // var result = java.callMethodSync(testInstance,"insertOrEditAProject",JSON.stringify(param));
    // res.send(result);

    java.callMethod(testInstance,"insertOrEditAProject",JSON.stringify(param),function(err, result){
        res.send(result);
    });

  }
**/
  function getMiTankDesc(req,res){

    var param = req.body;
    // var testInstance = java.newInstanceSync("com.vassarlabs.prapp.main.MiTankCrudController");
    // var result = java.callMethodSync(testInstance,"getMiTankDetails",JSON.stringify(param));
    // res.send(result);

    java.callMethod(testInstance,"getMiTankDetails",JSON.stringify(param),function(err, result){
        res.send(result);
    });
  }

  function getTransactionData(req,res){
      var param = req.body;
      // var testInstance = java.newInstanceSync("com.vassarlabs.prapp.main.MiTankCrudController");
      // var result = java.callMethodSync(testInstance,"getTransactionData",JSON.stringify(param));
      // res.send(result);
      java.callMethod(testInstance,"getTransactionData",JSON.stringify(param),function(err, result){
          res.send(result);
      });
    }

  function getMapDataForCapacity(req,res){
      // var testInstance = java.newInstanceSync("com.vassarlabs.prapp.main.MiTankCrudController");
      // var result = java.callMethodSync(testInstance,"getMainDashboardMapForCapacity");
      // res.send(JSON.parse(result));
      java.callMethod(testInstance,"getMainDashboardMapForCapacity",function(err, result){
          res.send(result);
      });
  }
  function getMapDataForPercentage(req,res){

      // var testInstance = java.newInstanceSync("com.vassarlabs.prapp.main.MiTankCrudController");
      // var result = java.callMethodSync(testInstance,"getMainDashboardMapForPercentage");
      // res.send(JSON.parse(result));

      java.callMethod(testInstance,"getMainDashboardMapForPercentage",function(err, result){
          res.send(result);
      });
  }


  function getImageById(req,res){
    var param = req.body;
    // var testInstance = java.newInstanceSync("com.vassarlabs.prapp.main.MiTankCrudController");
    // var result = java.callMethodSync(testInstance,"fileDownload",JSON.stringify(param));
    // res.send(result);
    java.callMethod(testInstance,"fileDownload",JSON.stringify(param),function(err, result){
        res.send(result);
    });
  }

   function getSearchData(req,res){
     var param = req.body;
     java.callMethod(testInstance,"searchMITank",JSON.stringify(param),function(err, result){
     res.send(result);
     });
   }

   function getReports(req,res){
     var param = req.body;
     java.callMethod(testInstance,"getReports",JSON.stringify(param),function(err, result){
     res.send(result);
     });
   }

    function getAdminReports(req,res){
      var param = req.body;
      java.callMethod(testInstance,"getStatusReports",JSON.stringify(param),function(err, result){
      res.send(result);
      });
    }

    function getBasinLevelData(req,res){
      var param = req.body;
      java.callMethod(testInstance,"getMiTankBasinLevelData",JSON.stringify(param),function(err, result){
      res.send(result);
      });
    }






}
