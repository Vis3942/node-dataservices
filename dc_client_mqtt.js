const fs = require("fs");
const mqtt = require('mqtt')

const { RotateFilesByDate }  = require('./RotateFiles');
const writeToCsv = require('./WriteToCsv');
var settings = null;
var node_times = {};
const reg_pat = /\{([\s\w]*)"Items":/gm;

var pending_get_cmds = []
var pending_ui_cmds = []
const node_data = {}
var one_day_secs = 86400;

let settings_pathfile = '';
if (fs.existsSync('/home/debian/config/node_dataservices/dc_client_settings.json')) {
  settings_pathfile = '/home/debian/config/node_dataservices/dc_client_settings.json'; 
}
else {
  settings_pathfile = "./dc_client_settings.json";
}
const node_schedule_disk_filename = "/home/debian/config/node_dataservices/node_schedules.json"
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

function cleanNodeName(dirty_node_name) {
  let ii = dirty_node_name.indexOf('_')
  let tmp_len = ii + 1 + 6;
  let tmpstr = dirty_node_name.slice(0, tmp_len);
  return tmpstr;
}


function sendToUI(obj) {
  const sendObj = formatDataForUI(obj);

  sendObj.Name = cleanNodeName(sendObj.Name)

  if (sendObj !== null)
  {
    if ( (mqtt_clients.hasOwnProperty(sendObj.Name)) && 
         (mqtt_connected.hasOwnProperty(sendObj.Name)) && 
         (mqtt_connected[sendObj.Name] === true)
    )
    {
      send_dict = {}
      sendObj.Data.forEach((x) => {
        send_dict[x.Name] = x.Value;
      })
      mqtt_clients[sendObj.Name].publish(topic, JSON.stringify(send_dict), { qos: 0, retain: false }, (error) => {
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
        sendObj.Name = dataObj.Items[0].NodeName;
        const tarr = []
        for (let item of dataObj.Items) {
          for (let arrayItem of item.Data) {
            tarr.push(arrayItem);
          }
        }
        sendObj.Data = tarr;
        node_data[sendObj.Name] = tarr;
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
if (fs.existsSync('/home/debian/config/node_dataservices/dc_mqtt_settings.json')) {
  mqtt_settings_pathfile = '/home/debian/config/node_dataservices/dc_mqtt_settings.json'; 
}
else {
  mqtt_settings_pathfile = "./dc_mqtt_settings.json";
}

try {
  const tmpdata = fs.readFileSync(mqtt_settings_pathfile);
  mqtt_settings = JSON.parse(tmpdata);
}
catch (err) {
  logger.error(err);
}

if ( (mqtt_settings !== null) && (mqtt_settings["schedulePeriodSecs"] !== undefined) )
{
  one_day_secs = mqtt_settings["schedulePeriodSecs"];
}

var mqtt_client = null

// const topic = '/nodejs/mqtt'
const topic = mqtt_settings.topic;
const topic_cmd = mqtt_settings.topic_cmd;
var mqtt_clients = {};
var mqtt_connected = {}; // keep track of status of the mqtt clients above
var on_schedule_task = {};
var off_schedule_task = {};

function scheduleOnOffTask(loc_node_id, onOffStr) {
  // format: [{"Node":"APIPM3_9910D9","Commands":[{"Name":"set_OnOff","Value":"0"}]}]
  logger.info("sch task " + onOffStr + " for node " + loc_node_id); 
  let onoff_val;
  if (onOffStr === "on") {
    onoff_val = "2";
  }
  else if (onOffStr === "off") {
    onoff_val = "1";
  }
  else {
    logger.info("Invalid schedule (on/off) received for node " + loc_node_id + " ---> " + onOffStr + ". No action taken");
    return;
  }
  tmpObj = {
    Node: loc_node_id,
    Commands: [
      {
        Name: "set_OnOff",
        Value: onoff_val
      }
    ]
  }
  let tstr = JSON.stringify([tmpObj]);
  if (dc_connected) {
    centralMasterPipeSocket.write(tstr + "\n");
    logger.info("Sent command to DC: " + tstr)
  }
  else
  {
    logger.info("DC not connected failed to send command: " + tstr);
  }

}

function connetToMQTTServer(username_key, node_id) {

  logger.info("Connect for [" + node_id + "] using key => " + username_key);

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
    username: username_key,
    reconnectPeriod: mqtt_settings.reconnectPeriod,
  })
  
  mqtt_clients[node_id].on('connect', () => {
    logger.info('Connected');
    mqtt_connected[node_id] = true;
    setDataAppState(true);
    mqtt_clients[node_id].subscribe([topic, topic_cmd + "+"], () => {
      logger.info(node_id + " | Subscribed to topic => " + topic);
      logger.info(node_id + " | Subscribed to topic => " + topic_cmd + "+");
    })

  })
 
  mqtt_clients[node_id].on('error', (err) => {
    setDataAppState(false);
    logger.info(err);
  })

  mqtt_clients[node_id].on('message', (recv_topic, recv_payload) => {
    logger.info('Received Message:' + recv_topic + " => " + recv_payload)
    let recv_obj = JSON.parse(recv_payload)
    // logger.info("payload type")
    // logger.info(typeof (recv_payload))
    if (recv_topic.startsWith(topic_cmd)) {
      if (recv_obj["method"].startsWith("get")) {
        let getdata_name = recv_obj["method"].slice(4)
        if (node_data.hasOwnProperty(node_id)) {
          let tmpObj = node_data[node_id].find(item => item.Name === getdata_name)
          if (tmpObj !== undefined) {
            let tsend_obj = {}
            tsend_obj[getdata_name] = parseFloat(tmpObj.Value)
            let tsend2_obj = {}
            tsend2_obj["params"] = tsend_obj;
            let tmpsend_topic = recv_topic.replace("request", "response")
            // mqtt_clients[node_id].publish(tmpsend_topic, JSON.stringify(tsend2_obj), { qos: 0, retain: false }, (error) => {
            // knob controls expect a string response not JSON
            mqtt_clients[node_id].publish(tmpsend_topic, tmpObj.Value, { qos: 0, retain: false }, (error) => {
              if (error) {
                logger.error(error)
              }
            })
          }
          else {
            logger.error("NOt found: Node id " + node_id + " - var " + getdata_name)
          }
        }
        else {
          logger.error("Node id " + node_id + " not found.")
        }
      }
      else if (recv_obj["method"].startsWith("set")) {
        let tobj = {}
        let value_str = "";
        tobj.Node = node_id;
        if (typeof(recv_obj["params"] === "object" ) ) {
          value_str = recv_obj["params"]["value"].toString()
        }
        else {
          value_str = recv_obj["params"].toString()
        }
        tobj.Commands = [
          {
            Name : recv_obj["method"],
            Value : value_str
          }
        ]
        let tstr = JSON.stringify([tobj]);
        if (dc_connected) {
          centralMasterPipeSocket.write(tstr + "\n");
          logger.info("Sent command to DC: " + tstr)
        }
        else
        {
          logger.info("DC not connected failed to send command: " + tstr);
        }
      }
      else if (recv_obj["method"] === "schedule" ) {
        logger.info("in schedule processing");
        logger.info(recv_obj["params"]["onTime"]);
        logger.info(recv_obj["params"]["offTime"]);
        // format of received message {"method":"schedule","params":{"onTime":"18:30","offTime":"06:15"}}
        if ( (recv_obj["params"]["onTime"] === undefined) || (recv_obj["params"]["offTime"] === undefined) ) {
          logger.info("schedule for Node " + node_id + "did not contain both onTime and offTime parems. Skipping scheduling");
          return;
        }
        
        let curr_date = new Date();
        let tmp_date_str = curr_date.toISOString().split('T')[0];
        let on_sch_date = new Date(tmp_date_str + "T" + recv_obj["params"]["onTime"] + ":00.000Z");
        let off_sch_date = new Date(tmp_date_str + "T" + recv_obj["params"]["offTime"] + ":00.000Z");
        if ( (isValidDate(on_sch_date)) && (isValidDate(off_sch_date)) ) {
          try {
              updateSchedulesFile(node_id, recv_obj);
              setupScheduleForNode(node_id, curr_date, on_sch_date, off_sch_date);
            }
          catch (err) {
            logger.info(err);
          }
        }
        else {
          logger.info("Invalid on off time recieved for Node. Expecting 24 hour format HH:MM only " + node_id  );
        }
      }
      
    }
  })
  
  mqtt_clients[node_id].on('disconnect', (packet) => {
    setDataAppState(false);
    logger.info(packet);
  })

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

function isValidDate(d) {
  return d instanceof Date && isFinite(d);
}

function loadSchedulesFile() {
  // return node_schs
  let node_schs;
  if (fs.existsSync(node_schedule_disk_filename)) {
  try {
    let data = fs.readFileSync(node_schedule_disk_filename, "utf8");      
    node_schs = JSON.parse(data);
  } catch (error) {
    logger.info(error);
      node_schs = {
        onTimes: {},
        offTimes: {}
      }
    }
  }
  else {
    node_schs = {
      onTimes: {},
      offTimes: {}
    }
  }
  return node_schs;
}

function updateSchedulesFile(loc_node_id, recv_sch_obj) {
  // format of received message {"method":"schedule","params":{"onTime":"18:30","offTime":"06:15"}}
  // this function is called only after the on time and off times are validated. No validation in this function
  let node_schs = loadSchedulesFile();
  // logger.info("in update sch file");
  // logger.info(Object.keys(node_schs["onTimes"]).toString());
  // logger.info(Object.keys(node_schs["offTimes"]).toString());
  // logger.info("node id [" + loc_node_id + "] on time " + recv_sch_obj["params"]["onTime"] + " off time " + recv_sch_obj["params"]["offTime"]);
  node_schs["onTimes"][loc_node_id] = recv_sch_obj["params"]["onTime"];
  node_schs["offTimes"][loc_node_id] = recv_sch_obj["params"]["offTime"];
  try {
    fs.writeFileSync(node_schedule_disk_filename, JSON.stringify(node_schs));
    logger.info("sucessfully updated schedule for node [" + loc_node_id + "]");
  } catch (error) {
    logger.info("Error updating schedule for node [" + loc_node_id + "]");
    logger.info(error);
  }
}

function setupScheduleForNode(loc_node_id, curr_date, on_sch_date, off_sch_date) {
  if (on_schedule_task[loc_node_id] !== undefined) {
    clearInterval(on_schedule_task[loc_node_id]);
  }  
  let init_delay_ms = on_sch_date.getTime() - curr_date.getTime();
  while (init_delay_ms < 0) {
    init_delay_ms = init_delay_ms + one_day_secs * 1000;
  }
  setTimeout(() => {
    scheduleOnOffTask(loc_node_id, "on");
    on_schedule_task[loc_node_id] = setInterval(scheduleOnOffTask, one_day_secs*1000, loc_node_id, "on");
  }, init_delay_ms);

  
  if (off_schedule_task[loc_node_id] !== undefined) {
    clearInterval(off_schedule_task[loc_node_id]);
  }  
  init_delay_ms = off_sch_date.getTime() - curr_date.getTime();
  while (init_delay_ms < 0) {
    init_delay_ms = init_delay_ms + one_day_secs * 1000;
  }
  setTimeout(() => {
    scheduleOnOffTask(loc_node_id, "off");
    off_schedule_task[loc_node_id] = setInterval(scheduleOnOffTask, one_day_secs*1000, loc_node_id, "off");
  }, init_delay_ms);

}

function setupSchedulesOnAppStart() {
  let node_schs = loadSchedulesFile();
  let on_time_keys = Object.keys(node_schs.onTimes).sort();
  let off_time_keys = Object.keys(node_schs.offTimes).sort();
  let on_time_len = on_time_keys.length;
  let off_time_len = off_time_keys.length;
  if ( (on_time_len === 0) || (off_time_len === 0) ) {
    logger.info("No schedules in file");
    return;
  }
  if ( (on_time_len !== off_time_len) ) {
    logger.info("invalid schedules read from file");
    logger.info("on time nodes: " + on_time_keys);
    logger.info("off time nodes: " + off_time_keys);
    return;
  }

  // verify that all nodes names are identical
  let status = true;
  for (let i = 0; i < on_time_keys.length; i++) {
    if (on_time_keys[i] !== off_time_keys[i]) {
      status = false;
      break;
    }
  }
  if ( (status === false) ) {
    logger.info("Unequal nodes read from file");
    logger.info("on time nodes: " + on_time_keys);
    logger.info("off time nodes: " + off_time_keys);
    return;
  }

  let curr_date = new Date();
  let tmp_date_str = curr_date.toISOString().split('T')[0];
  for (const tmp_node_id in node_schs["onTimes"]) {
    let on_sch_date = new Date(tmp_date_str + "T" + node_schs["onTimes"][tmp_node_id] + ":00.000Z");
    let off_sch_date = new Date(tmp_date_str + "T" + node_schs["offTimes"][tmp_node_id] + ":00.000Z");
    if ( (isValidDate(on_sch_date)) && (isValidDate(off_sch_date)) ) {
      setupScheduleForNode(tmp_node_id, curr_date, on_sch_date, off_sch_date);
    }
    else {
      logger.info("Invalid on off time recieved for Node. Expecting 24 hour format HH:MM only " + tmp_node_id  );
    }

  }

}

mqtt_settings.AccessKeys.forEach(element => {
    connetToMQTTServer(element.Key, element.nodeId);
});

establishConnection();
// main funtions
makeCentralMasterConnection();
setupSchedulesOnAppStart();
  
