var express        = require('express'),
    bodyParser     = require('body-parser'),             //Middle ware for parsing data like JSON data
    methodOverride = require('method-override'),
    cookieParser   = require('cookie-parser'),
    compress    = require('compression'),
    java           = require('java'),
    formidable     = require('formidable'),
    util           = require('util'),
    session        = require('express-session'),
    router         = express.Router(), //using express router
    routeIndex     = require('./routes/index')(),
    port           = process.env.PORT || 3002;             //setting port
    app            = module.exports = express();
    http           = require('http').Server(app);
    oneDay         = 86400000;




/**** Configurations ***/
/**
  * NOTE
  *  router should be used after bodyparser
  **/
app.use(compress());
app.engine('html', require('ejs').renderFile);    // use ejs to render html
app.use(bodyParser.json());                       // parse application/json pull information from html in POST
app.use(bodyParser.raw({ type: 'application/vnd.custom-type' }));
// parse an HTML body into a string
app.use(bodyParser.text());
app.use(methodOverride());                        // simulate DELETE and PUT
app.use(cookieParser());
app.use(session({
    secret: '34SDgsdgspxxxxxxxdfsG', // just a long random string
    resave: false,
    saveUninitialized: true
}));
   // use ejs to render html
app.use(express.static(__dirname + '/public',{maxAge: oneDay}));   // set the static files location /public/img will be /img for users
app.set('views', __dirname + '/views');
app.use("/img", express.static(__dirname + '/img'));
app.use('/', router);

router.options('*', function(req, res, next) {
	//console.log('in options...');
  //res.setHeader('Access-Control-Allow-Origin', 'http://localhost:8888');
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Authentication, Content-Type, X-Requested-With");
  res.header("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS, HEAD");
  res.header('Content-Type', 'application/json');
  res.header('Access-Control-Allow-Credentials', true);
  return next();
});

router.all("/api/*", function(req, res, next) {
//	console.log('in router.all ...');
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Authentication, Content-Type, X-Requested-With");
  res.header("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS, HEAD");
  res.header('Content-Type', 'application/json');
  res.header('Access-Control-Allow-Credentials', true);

  //res.header('Content-Type', 'text/plain');
  return next();
});


router.get('/', routeIndex.index);
router.get('/partials/irrigation/:name', routeIndex.irrigation);
router.get('/partials/transco/:name', routeIndex.transco);
router.get('/partials/mitank/:name', routeIndex.mitank);
router.get('/partials/auth/:name',routeIndex.auth);
router.get('/partials/:name',routeIndex.partials)
router.get('/partials/kuppam/:name', routeIndex.kuppam);
router.get('/partials/ewet/:name', routeIndex.ewet);
router.get('/partials/rb/:name', routeIndex.rb);


/**
*prapp
*/
//java.classpath.push('/home/vassarlabs50/dev/prapp/prapp2/prapp/server/target/priapp-server-0.0.1-SNAPSHOT.jar');
//java.classpath.push('/home/ec2-user/deploy/prapp/nodeServer/priapp-server-0.0.1-SNAPSHOT.jar');
java.classpath.push('/home/lenovo/vassarlabs/master/prapp/server/target/priapp-server-0.0.1-SNAPSHOT.jar');

/**
*prapp-mit
*/
//java.classpath.push('/home/vaibhav/vassarlabs/devlopment/rbLatest/prapp/server/target/priapp-server-0.0.1-SNAPSHOT-jar-with-dependencies.jar');
//java.classpath.push('/home/ec2-user/deploy/prapp-mit/nodeServer/priapp-server-0.0.1-SNAPSHOT-jar-with-dependencies.jar');
//java.classpath.push('/home/ubuntu/deploy/prapp_mit/nodeServer/priapp-server-0.0.1-SNAPSHOT-app.jar');

var testInstance = java.newInstanceSync("com.vassarlabs.prapp.main.PrappMain");
java.callMethodSync(testInstance,"appInitApp");

var sessionInstance = java.newInstanceSync("com.vassarlabs.prapp.session.PRAppSessionHandler");

var restService    = require('./routes/services/restService')(java,sessionInstance);
var restServiceEncrypt  = require('./routes/services/restServicesEncrypted')(java,sessionInstance,formidable,util);
var irrigationService = require('./routes/services/irrigation.service')(java,sessionInstance);
var transcoService = require('./routes/services/transco.service')(java,sessionInstance);
var mitankService = require('./routes/services/mitank.service')(java);
var kuppamService = require('./routes/services/kuppam.service')(java);
var ewetService = require('./routes/services/ewet.service')(java);
var rbService = require('./routes/services/rb.service')(java);

router.post('/api/login',restService.getLoginDetails);

//console.log(irrigationService.getReportData());
/** REST operation for ui **/
router.post('/api/irrigation/p1/c1',irrigationService.getTodaysProgressMain);
router.post('/api/irrigation/p1/stk1',irrigationService.getReportedZeroProgressMain);
router.post('/api/irrigation/p1/pi1',irrigationService.getIIrrIssuesChartDataMain);
router.post('/api/irrigation/p1/picol1',irrigationService.getIIrrCumulativeProgressMain);
router.post('/api/irrigation/p1/npckg',irrigationService.getTotalNoOfPackages);
router.post('/api/irrigation/p1/c5',irrigationService.getWeeklyProgress);
router.get('/api/irrigation/p1/plist',irrigationService.getPPList);
router.post('/api/irrigation/p1/exdate',irrigationService.getExpectedCompletion);
router.post('/api/irrigation/p1/transaction',irrigationService.getTransactionData);
router.post('/api/irrigation/p1/tabdata',irrigationService.getTabularData);
router.post('/api/irrigation/p1/pclist',irrigationService.getPCList);
router.post('/api/irrigation/p1/rptdata',irrigationService.getReportData);


/** REST operation for Transco ui **/
router.post('/api/transco/p1/c1',transcoService.getTodaysProgressMain);
router.post('/api/transco/p1/stk1',transcoService.getReportedZeroProgressMain);
router.post('/api/transco/p1/pi1',transcoService.getIIrrIssuesChartDataMain);
router.post('/api/transco/p1/picol1',transcoService.getStageWiseAchievedVsBalanceDataMain);
router.post('/api/transco/p1/picol2',transcoService.getKmsOfTransmissionLineElectrifiedDataMain);
router.post('/api/transco/p1/npckg',transcoService.getTotalNoOfPackages);
router.post('/api/transco/p1/c5',transcoService.getWeeklyProgress);
router.post('/api/transco/p1/plist',transcoService.getPPList);
router.post('/api/transco/p1/exdate',transcoService.getExpectedCompletion);
router.post('/api/transco/p1/tabdata',transcoService.getTabularData);
router.post('/api/transco/p1/islist',transcoService.getIssuesList);
//
// /** Rest seervices For Mi getAdminReports',restServiceEncrypt.getDBLoginDetails); **/
router.post('/api/mitank/main/table',mitankService.getMiTankTableData);
router.post('/api/mitank/main/mitankdesc',mitankService.getMiTankDesc);
router.post('/api/edblogin',restServiceEncrypt.getDBLoginDetails);
router.post('/api/mitank/main/mitanksTransaction',mitankService.getTransactionData);
//router.post('/api/mitank/main/map',mitankService.getMapData);
router.post('/api/mitank/main/map',mitankService.getMapDataForCapacity);
router.post('/api/mitank/main/map2',mitankService.getMapDataForPercentage);
router.post('/api/mitank/main/list',mitankService.getAllMitanksofUser);
router.post('/api/mitank/main/ntmitank',mitankService.submitNotMitank);
//router.post('/api/mitank/main/dpmitank',mitankService.submitDuplicateTank);
router.post('/api/mitank/main/lhd',mitankService.getLocationHierarchyData);
//router.post('/api/mitank/main/snt',mitankService.submitNewTank);
router.post('/api/mitank/main/image',mitankService.getImageById);
router.post('/api/mitank/main/search',mitankService.getSearchData);
router.post('/api/mitank/main/report',mitankService.getReports);
router.post('/api/mitank/admin/addoreditproject',mitankService.insertOrEditAProject);
router.post('/api/mitank/admin/addoredituser',mitankService.insertOrEditAUser);
router.post('/api/mitank/admin/assigntank',mitankService.assignOrReassignProjects);
router.post('/api/mitank/admin/deletetank',mitankService.deleteProjects);
router.post('/api/mitank/admin/users',mitankService.getAllUsers);
router.post('/api/mitank/admin/designation',mitankService.getUsersDesignations);
router.post('/api/mitank/admin/tlh',mitankService.getTanksListInaHierarchy);
router.post('/api/mitank/admin/uh',mitankService.getUsersInaHierarchy);
router.post('/api/mitank/admin/report',mitankService.getAdminReports);
router.post('/api/mitank/basinlevel/table',mitankService.getBasinLevelData);

/** Rest seervices For kuppam**/
router.post('/api/kuppam/p1/pi',kuppamService.getPieChartData);
router.post('/api/kuppam/p1/npckg',kuppamService.getProjectCount);
router.post('/api/kuppam/p1/c1',kuppamService.getTodaysProgress);
router.post('/api/kuppam/p1/npad',kuppamService.getTotalNoOfPackagesAndDept);
router.post('/api/kuppam/p1/c5',kuppamService.getWeeklyProgress);
router.post('/api/kuppam/p1/stk1',kuppamService.getReportedZeroProgress);
router.post('/api/kuppam/p1/picol1',kuppamService.getCumulativeProgress);
router.post('/api/kuppam/p1/tabdata',kuppamService.getTabularData);
router.post ('/api/kuppam/p1/rttabdata',kuppamService.getTabularDataAtRootLevel);
router.post('/api/kuppam/p1/transaction',kuppamService.getTransactionData);
router.post('/api/kuppam/p1/pckdet',kuppamService.getPackageDetails);
router.post('/api/kuppam/p1/image',kuppamService.getImageById);

/** Rest services for ewet**/

router.post('/api/ewet/main/map',ewetService.getMapData);
router.post('/api/ewet/main/table',ewetService.getTabularData);
router.post('/api/ewet/main/packdetail',ewetService.getPackageDetails);
router.post('/api/ewet/main/image',ewetService.getImageById);
router.post('/api/ewet/main/transaction',ewetService.getTransactionData);



/** rest service for rb**/
router.get('/api/rb/p1/hlist',rbService.getHodList);
router.post('/api/rb/p1/summary',rbService.getSummaryData);
router.post('/api/rb/p1/stk1',rbService.getReportedZeroProgress);
router.post('/api/rb/p1/pi',rbService.getIssueChartData);
router.post('/api/rb/p1/picol1',rbService.getCumulativeProgress);
router.post('/api/rb/main/table',rbService.getTabularData);
router.post('/api/rb/main/packdetail',rbService.getPackageDetails);
router.post('/api/rb/main/image',rbService.getImageById);
router.post('/api/rb/main/transaction',rbService.getTransactionData);


router.get('*', routeIndex.index);


http.listen(port);
