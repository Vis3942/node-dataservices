const spawn = require('child_process').spawn;
const fs = require("fs");


var settings = null;
const settings_filename = "dc_client_settings.json";
let settings_pathfile = '';
let tmp_fstr = '/home/debian/config/node_dataservices/' + settings_filename ;
if (fs.existsSync(tmp_fstr) ) {
  settings_pathfile = tmp_fstr; 
}
else {
  settings_pathfile = "./" +  settings_filename;
}

try {
  const tmpdata = fs.readFileSync(settings_pathfile);
  settings = JSON.parse(tmpdata);
}
catch (err) {
  logger.error("dc_main: " + err);
  process.exit(1);
}

// settings.MainAppType values supported are:
//      Local: run the local js file dc_client_discover and to send data to local ui
//      ModbusServer: runs the js file that populates the modbus server with the data
//      MqttThingsBoard: runs the js file that sends data to ThingsBoard cloud using the "default" profile mqtt comms 
//      MqttCustom: runs the js file that sends data to custom server

const logger = require('./dc_client_logger')

let child = null;
let app_name = "None";
if (settings.MainAppType === "Local")
{
  app_name = "dc_client_discover.js";
}
else if (settings.MainAppType === "ModbusServer")
{
  app_name = "dc_modbus_server.js";
}
else if (settings.MainAppType === "MqttThingsBoard")
{
  app_name = "dc_client_mqtt.js";
}
else if (settings.MainAppType === "MqttCustom")
{
  app_name = "dc_client_mqtt_custom.js";
  //exit(-1);
}


if (app_name !== "None")
{
  child = spawn('node', [app_name]);
  child.on('error', ( err ) => {
    throw `failed to start command "${ app_name }": "${ err }"`;
  });
  child.on('close', function (code) {
    if (code !== 0) {
      logger.info("dc_main: " + app_name + ' process exited with code ' + code);
    }
  });
}
else 
{
  logger.error("dc_main: Unsupported app [" + settings.MainAppType + "] provided. Cannot start app.");
}

//const child = spawn('pwd');