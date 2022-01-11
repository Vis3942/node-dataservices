const net = require("net");
//const { stat } = require("fs");
const logger = require('./dc_client_logger');
const fs = require('fs');
var statusPipeClient = null;
var sts_interval = null;
var settings = null;

let settings_pathfile = '';
if (fs.existsSync('/home/debian/config/node_dataservices/dc_client_settings.json')) {
  settings_pathfile = '/home/debian/config/node_dataservices/dc_client_settings.json'; 
}
else {
  settings_pathfile = "./dc_client_settings.json";
}

try {
  const tmpdata = fs.readFileSync(settings_pathfile);
  settings = JSON.parse(tmpdata);
}
catch (err) {
  console.log(err);
}


function establishConnection() {
    if (statusPipeClient !== null) {
        statusPipeClient.destroy();
        statusPipeClient = null;
    }

    statusPipeClient = net.connect("/tmp/CoreFxPipe_" + settings.StatusPipeConnectionName, function () {
        logger.info("connected to status app");
    });

    if (sts_interval !== null) {
        clearInterval(sts_interval)
        sts_interval = null;
    }

    setInterval(pingStatus, settings.StatusPingIntervalSeconds*1000);

    statusPipeClient.on("error", function (error) {
        logger.info(error);
        statusPipeClient.close();
        logger.info("reconnecting... to status pipe");
        establishConnection();
    });
    
}

var dataAppState = settings.StatusAppFalseState;

function pingStatus() {
    statusPipeClient.write(dataAppState + "\r\n");
}

function setDataAppState(status) {
    if (status) {
        dataAppState = settings.StatusAppTrueState;
        logger.info("data app state set to true");
    }
    else {
        dataAppState = settings.StatusAppFalseState;
        logger.info("data app state set to false");
    }
}


function endConnection () {
    if (statusPipeClient !== null) {
        statusPipeClient.destroy();
        statusPipeClient = null;
    }
    if (sts_interval !== null) {
        clearInterval(sts_interval)
        sts_interval = null;
    }
}

//establishConnection();

//export { establishConnection, endConnection, setDataAppState}

module.exports = { establishConnection, endConnection, setDataAppState }