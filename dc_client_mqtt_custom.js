// currently this file is same as dc_clioent_mqtt.js
//  Todo: change this to send data to the custom mqtt server with topics specified in the settings file 
const fs = require("fs");
const mqtt = require('mqtt')

const { RotateFilesByDate }  = require('./RotateFiles');
const writeToCsv = require('./WriteToCsv');
var settings = null;
var node_times = {};
const reg_pat = /\{([\s\w]*)"Items":/gm;

var pending_get_cmds = []
var pending_ui_cmds = []

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
  logger.error(err);
  process.exit(1);
}

const logger = require('./dc_client_logger')
const { establishConnection, endConnection, setDataAppState } = require('./dc_client_pipestatus');

function addUICommandsToPendingList(cmds) {
  // add command to pending_ui_cmds, but delete any previous commands with the same variable
  // remove duplicates
  tmp_list = [];
  for (let ii=0; ii++; ii < pending_ui_cmds.length) {
    tobj = pending_ui_cmds[ii];
    if ( (tobj.Node === cmds.Node) && (tobj.UUID === cmds.UUID) ) {
      for (let jj=cmds.Commands.length - 1; jj--; jj < 0) {
        // assumpiton: the Commands array will have unique Name for the command, no duplicates
        let tmp_idx = tobj.Commands.findIndex(x => x.Name === cmds.Commands[ii].Name);
        if (tmp_idx >= 0) {
          tobj.Commands.splice(tmp_idx, 1);
        }
      }
      if (tobj.Commands.length > 0) {
        tmp_list.push(tobj);
      }
    }
    else {
      tmp_list.push(tobj);
    }
  }
  pending_ui_cmds = tmp_list;
}

// Using a single function to handle multiple signals
function handle(signal) {
  logger.info("app exit: closing pipeclient")
  //console.log("closing pipeclient");
  // if (centralMasterPipeClient !== null) {
  //   centralMasterPipeClient.destroy();
  //   centralMasterPipeClient = null;
  // }
  centralMasterPipeSocket.destroy();
  endConnection();
  process.exit(0);
}

process.on('SIGINT', handle);
process.on('SIGTERM', handle);

var net = require("net");
//const { clusterSettings } = require("cluster");
var recv_data = "";
var got_json = true;
let retrying = false;

function makeCentralMasterConnection() {
  centralMasterPipeSocket.connect("/tmp/CoreFxPipe_" + settings.PipeConnectionName);
}

function connectEventHandler() {
  logger.info("connected to CentralMaster socket");
  //console.log("connected to CentralMaster socket");
  dc_connected = true;
  retrying = false;
  if (pending_get_cmds.length > 0) {
    for (let ii=0; ii < pending_get_cmds.length; ii++)
    {
      centralMasterPipeSocket.write('get_cmds:' + pending_get_cmds[ii] + "\n");
    }
    pending_get_cmds = [];
  }
}

function disconnectEventHandler() {
  logger.info("disconnected from CentralMaster socket");
  //console.log("connected to CentralMaster socket");
  dc_connected = false;
}


function WriteToCsv(node_data) {
  writeToCsv.WriteToCsvFile(node_data);
  let tmpDate = new Date();
  let performRoateCheck = false;
  if (node_times[node_data.Name] === undefined) {
      node_times[node_data.Name] = tmpDate;
      // app just started; good to check file rotation
      performRoateCheck = true;
  } 
  else {
      if (node_times[node_data.Name].getUTCDate() !== tmpDate.getUTCDate()) {
          node_times[node_data.Name] = tmpDate;
          performRoateCheck = true;
      }
  } 
  if (performRoateCheck) {
      RotateFilesByDate(settings.numberOfFilesPerNode, 
          node_data.Name + "_" + settings.logPrefix, 
          "csv");

  } 
}


function findValidJson(str) {
  let matches_idx = [];
  let done_searching = false;
  while (!done_searching) {
    let tmp_match = reg_pat.exec(str);
    if (tmp_match !== null) {
      matches_idx.push(tmp_match.index)
    }
    else 
    {
      done_searching = true;
    }
  }
  // let matches = str.matchAll(reg_pat);
  // let match_arr = Array.from(matches);
  let close_brac_index = -1;
  let new_start_index = -1;
  var json_idx = [];
  matches_idx.forEach(function (index) {
    close_brac_index = findValidClosingBracket(str, index);
    //console.log("start index " + index);
    //console.log(str[index]);
    //console.log("close brac index " + close_brac_index);
    if (close_brac_index !== -1) {
      ////console.log(str.slice(index, close_brac_index+1));
      //console.log(JSON.parse(str.slice(index, close_brac_index+1)));
      json_idx.push(index);
      json_idx.push(close_brac_index);
    }
    else {
      new_start_index = index;
    }
  });
  // console.log("remainder: \n" + str.slice(new_start_index));
  return json_idx;
}

function findValidClosingBracket(str, pos) {
  if (str[pos] !== '{') {
    logger.info("did not find open bracket at " + pos);
    return -1;
    //throw new Error('The position must contain an opening bracket');
  }
  let level = 1;
  for (let index = pos + 1; index < str.length; index++) {
    if (str[index] === '{') {
      level++;
      //console.log("index : " + index + " level" + level);
    } else if (str[index] === '}') {
      level--;
      //console.log("index : " + index + " level" + level);
    }
    if (level === 0) {
      return index;
    }
  }
  return -1;
}
  
function sendToUI(obj) {
  const sendObj = formatDataForUI(obj);

  if (sendObj !== null)
  {
    if ( (mqtt_clients.hasOwnProperty("customclient")) && 
         (mqtt_connected.hasOwnProperty("customclient")) && 
         (mqtt_connected["customclient"] === true)
    )
    {
      // send_dict = {}
      // sendObj.Data.forEach((x) => {
      //   send_dict[x.Name] = x.Value;
      // })
      mqtt_clients["customclient"].publish(topic, JSON.stringify(sendObj), { qos: 0, retain: false }, (error) => {
        if (error) {
          logger.error(error)
        }
      })
    }
    else
    {
      logger.info("Got data for UI (Not sent) " + sendObj.Data.length + " items from " + sendObj.Name);
    }
    WriteToCsv(sendObj);
  }

}

function dataEventHandler(data) {
  recv_data = recv_data + data;
  logger.info("data len : " + recv_data.length);
  json_beg_end_idx = findValidJson(recv_data);
  if ( (json_beg_end_idx.length > 0) && (json_beg_end_idx.length % 2 === 0) ) {
    for (ii=0; ii < json_beg_end_idx.length; ii = ii + 2) {
      //console.log("json object [" + json_beg_end_idx[ii] + "] to [" + json_beg_end_idx[ii+1] + "]");
      const obj = JSON.parse(recv_data.slice(json_beg_end_idx[ii], json_beg_end_idx[ii+1] + 1));
      sendToUI(obj);
    }
    // debug only
    //console.log("Remainder:");
    let last_idx = json_beg_end_idx.pop() + 1; // removes the last element to a variable
    recv_data = recv_data.slice(last_idx);
  }
  else 
  {
    if (json_beg_end_idx.length % 2 !== 0) {
      logger.log("Invalid index array returned from parsing recvd data.");
      logger.log(json_beg_end_idx);
    }
  }
}

function errorEventHandler(err) {
    logger.error(err);
    //console.log("reconnecting... to cm pipe");
    dc_connected = false;
  }
  
  function closeEventHandler() {
    if (!retrying) {
      retrying = true;
      logger.info('reconnecting... to central master');
    }
    setTimeout(makeCentralMasterConnection, settings.PipeConnectionTimeoutSecs*1000);
    dc_connected = false;
  }
  
  let centralMasterPipeSocket = net.Socket();
  centralMasterPipeSocket.on('connect', connectEventHandler);
  centralMasterPipeSocket.on('data', dataEventHandler);
  centralMasterPipeSocket.on('error', errorEventHandler);
  centralMasterPipeSocket.on('close', closeEventHandler);
  centralMasterPipeSocket.on('disconnect', disconnectEventHandler);
  
  function formatDataForUI(dataObj) {
    // data recv as ["Items": [ {NodeName: xxxx, UUID: xxxx, Data[]},
    //                          {NodeName: xxxx, UUID: xxxx, Data[]},
    //                          {NodeName: xxxx, UUID: xxxx, Data[]}]
    //   where Data array of objects of type 
    //   { Name: "xxxx", Value: "xxxx",  Timestamp: "xxxx", Event: "xxxx" }
    // this function formats this as
    // { Name: <node name>,
    //   Data: [
    //     {Name: <var name>, Value: <value of var>, Timestamp: <time str>, Event: <indicator>},
    //     {Name: <var name>, Value: <value of var>, Timestamp: <time str>, Event: <indicator>},
    //     {Name: <var name>, Value: <value of var>, Timestamp: <time str>, Event: <indicator>}
    //   ]
    // }
  
    // for set of commands, json receiveed as 
    // data recv as ["Commands": [ {NodeName: xxxx, NodeCommands: [{UUID: xxxx, set_cmds[] }, UUID: xxxx, set_cmds: [] ] },
    //                             {NodeName: xxxx, NodeCommands: [{UUID: xxxx, set_cmds[] }, UUID: xxxx, set_cmds: [] ] },
    //                             {NodeName: xxxx, NodeCommands: [{UUID: xxxx, set_cmds[] }, UUID: xxxx, set_cmds: [] ] }]
    //   where set_cmds element 
    //   { Name: "xxxx", description: "xxxx",  type: "xxxx", value: "xxxx", min: "", max: "" }
    //  this data is sent as is to the UI
  
    if (dataObj.Items !== undefined) {
      if (dataObj.Items.length > 0) {
        const sendObj = {}
        sendObj.Node = dataObj.Items[0].NodeName;
        const tarr = []
        for (let item of dataObj.Items) {
          for (let arrayItem of item.Data) {
            tarr.push(arrayItem);
          }
        }
        sendObj.Data = tarr;
        return sendObj;
      } else {
        logger.error("No data to format, array length is 0")
        return null;
      }
    }
    else if (dataObj.Commands !== undefined)
    {
      if (dataObj.Commands.length > 0) {
        return dataObj;
      } else {
        logger.error("No commands to to send, array length is 0")
        return null;
      }
    }
}

// mqtt functions here
var mqtt_settings = null;
var sts_interval = null;


// TODO: redirect all console log to logger

let mqtt_settings_pathfile = '';
if (fs.existsSync('/home/debian/config/node_dataservices/dc_mqtt_custom_settings.json')) {
  mqtt_settings_pathfile = '/home/debian/config/node_dataservices/dc_mqtt_custom_settings.json'; 
}
else {
  mqtt_settings_pathfile = "./dc_mqtt_custom_settings.json";
}

try {
  const tmpdata = fs.readFileSync(mqtt_settings_pathfile);
  mqtt_settings = JSON.parse(tmpdata);
}
catch (err) {
  logger.error(err);
}

var mqtt_client = null

// const topic = '/nodejs/mqtt'
const topic = mqtt_settings.topic;
const topic_cmd = mqtt_settings.topic_cmd;
var mqtt_clients = {};
var mqtt_connected = {}; // keep track of status of the mqtt clients above

function connetToMQTTServerCustom(node_id) {

  logger.info("Connect for [" + node_id + "] using defalut conn keys ");

  if (mqtt_clients.hasOwnProperty(node_id))
  {
    if (mqtt_clients[node_id] !== null)
    {
      mqtt_clients[node_id].end;
      mqtt_clients[node_id] = null;
      mqtt_connected[node_id] = false;
    }
  }

  let connectUrl = `mqtt://${mqtt_settings.CloudHost}:${mqtt_settings.CloudPort}`

  let clientId = `mqtt_${Math.random().toString(16).slice(6)}`

  mqtt_clients[node_id] = mqtt.connect(connectUrl, {
    clientId,
    clean: true,
    connectTimeout: 4000,
    username: mqtt_settings.username,
    password: mqtt_settings.password,
    reconnectPeriod: mqtt_settings.reconnectPeriod,
  })
  
  mqtt_clients[node_id].on('connect', () => {
    logger.info('Connected');
    mqtt_connected[node_id] = true;
  
    mqtt_clients[node_id].subscribe([topic_cmd ], () => {
      logger.info(node_id + " | Subscribed to topic => " + topic_cmd );
    })

  })
 
  mqtt_clients[node_id].on('error', (err) => {
    logger.info(err);
  })

  mqtt_clients[node_id].on('message', (recv_topic, recv_payload) => {
    logger.info('Received Message:' + recv_topic + " => " + recv_payload)
    if (dc_connected) {
      let tsend_obj = JSON.parse(recv_payload)
      let tstr = JSON.stringify([tsend_obj])
      centralMasterPipeSocket.write(tstr + "\n");
      logger.info("Sent command to DC" )
    }
    else
    {
      logger.info("DC not connected failed to send command.");
    }
  })
  
  mqtt_clients[node_id].on('disconnect', (packet) => {
    logger.info(packet);
  })
  
  //mqtt_clients[node_id].on('message', (topic, payload) => {
  //  logger.info('Received Message:', topic, payload.toString())
  //})

  mqtt_clients[node_id].on('close', () => {
    disconnectFromMQTTServer(node_id);
  })
  
}

function disconnectFromMQTTServer (node_id) {
    logger.info(node_id + " :  in disconnectfromMQTT func");
    if (mqtt_clients.hasOwnProperty(node_id))
    {
      if (mqtt_clients[node_id] !== null)
      {
        //mqtt_clients[node_id].end();
        //delete mqtt_clients[node_id];
        mqtt_connected[node_id] = false;
      }
    }
}

// mqtt_settings.AccessKeys.forEach(element => {
//     connetToMQTTServerCustom(element.nodeId);
//   });
// a single connection for all nodes
connetToMQTTServerCustom("customclient");
  
// main funtions
makeCentralMasterConnection();

  
