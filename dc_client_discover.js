const io = require("socket.io-client");
const moment = require("moment");
const dgram = require("dgram");
var udpSocket = dgram.createSocket("udp4");
//const settings = require("./dc_client_settings.json");
const process =require('process');
const { send } = require("process");
const fs = require("fs");
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
  console.log(err);
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
    if ( (ui_connected) && (socket !== null) ) {
      if (sendObj.Commands !== undefined) {
        socket.emit('node_cmds', sendObj);
        logger.info("Sending to ui " + sendObj.Commands[0].NodeName + " items from " + sendObj.Name);
      } else {
        socket.emit('node_data', sendObj);
        logger.info("Sending to ui " + sendObj.Data.length + " items from " + sendObj.Name);
      }
    } 
    else {
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


// function dataEventHandler(data) {
//   if (got_json) {
//     recv_data = data;
//     //console.log("creating recv_data with ");
//     //console.log(recv_data);
//     //console.log("-------------");
//   } else {
//     //console.log("appending data");
//     recv_data = recv_data + data;
//     //console.log(recv_data);
//     //console.log(" xxxxxxxxxxxxxxxxx    ");
//   }
//   got_json = testJSON(recv_data);
//   //console.log("got_json = " + got_json);
//   if (got_json) {
//     const obj = JSON.parse(recv_data);
//     const sendObj = formatDataForUI(obj);
//     //console.log(sendObj);
//     recv_data = '';
//     if (sendObj !== null)
//     {
//       if ( (ui_connected) && (socket !== null) ) {
//         socket.emit('node_data', sendObj);
//         logger.info("Sending to ui " + sendObj.Data.length + " items from " + sendObj.Name);
//       } 
//       else {
//         logger.info("Got data for UI (Not sent) " + sendObj.Data.length + " items from " + sendObj.Name);
//       }
//       WriteToCsv(sendObj);
//     }
//   } 
//   // else {
//   //   console.log(recv_data);
//   // }
// }

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

// var centralMasterPipeClient = net.connect("/tmp/CoreFxPipe_PLSDCConnection", function () {
//   console.log("connected to server");
// });

var ui_connected = false;
var dc_connected = false;

// function testJSON(text) {
//   if (typeof text !== "string") {
//     return false;
//   }
//   try {
//     JSON.parse(text);
//     return true;
//   } catch (error) {
//     return false;
//   }
// }

function formatDataForUI(dataObj) {
  // data recv as ["Items": [ {NodeName: xxxx, UUID: xxxx, Data[]},
  //                          {NodeName: xxxx, UUID: xxxx, Data[]},
  //                          {NodeName: xxxx, UUID: xxxx, Data[]}]
  //   where Data array of objects of type 
  //   { Name: "xxxx", Value: "xxxx",  Timestamp: "xxxx", Event: "xxxx" }
  // this function formats this as
  // { Node: <node name>,
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

// centralMasterPipeClient.on("data", function (data) {
//   if (got_json) {
//     recv_data = data;
//     //console.log("creating recv_data with ");
//     //console.log(recv_data);
//     //console.log("-------------");
//   } else {
//     //console.log("appending data");
//     recv_data = recv_data + data;
//     //console.log(recv_data);
//     //console.log(" xxxxxxxxxxxxxxxxx    ");
//   }
//   got_json = testJSON(recv_data);
//   //console.log("got_json = " + got_json);
//   if (got_json) {
//     const obj = JSON.parse(recv_data);
//     const sendObj = formatDataForUI(obj);
//     //console.log(sendObj);
//     recv_data = '';
//     if (sendObj !== null)
//     {
//       console.log("Sending to ui " + sendObj.Data.length + " items from " + sendObj.Name);
//       if ( (ui_connected) && (socket !== null) ) {
//         socket.emit('node_data', sendObj);
//       }
//     }
//   } 
//   // else {
//   //   console.log(recv_data);
//   // }
// });

// centralMasterPipeClient.on("error", function (error) {
//   console.log(error);
//   centralMasterPipeClient.destroy();
//   console.log("reconnecting... to cm pipe");
//   // TODO: RESTRUCTURE CODE FOR reconnections
// });

udpSocket.on("listening", function () {
  var address = udpSocket.address();
  logger.info("server listening " + address.address + ":" + address.port);
});

var connect_to_address = "";
var connect_to_port = 0;

let socket = null;
//let interval;

udpSocket.on("message", function (msg, rinfo) {
  logger.info(
    "server got: " + msg + " from " + rinfo.address + ":" + rinfo.port
  );
  let tobj = JSON.parse(msg);
  logger.verbose(tobj.address);
  logger.verbose(tobj.port);
  if (connect_to_port > 0 && connect_to_address.length > 0) {
    if (connect_to_address === tobj.address && connect_to_port === tobj.port) {
      return;
    } else {
      // already connected to a different addr: port, so disconnect
      socket.disconnect();
      socket = null;
      connect_to_address = tobj.address;
      connect_to_port = tobj.port;
    }
  } else {
    connect_to_address = tobj.address;
    connect_to_port = tobj.port;
  }

  socket = io.connect("http://" + connect_to_address + ":" + connect_to_port, {
    transports: ["websocket", "polling"],
    reconnect: true,
  });

  socket.on("connect", (data) => {
    logger.info("connected ...to socket-io server");
    socket.emit("identity", "dc");
    ui_connected = true;
    setDataAppState(true);
    // if (interval) {
    //   clearInterval(interval);
    // }

    // interval = setInterval(() => {
    //   var timestr = moment.utc().format("MM-DD-YYYY HH:mm:ss") + " +00:00"; // utc now() -> 2020-03-20 14:32:20
    //   const node_vars = ["Temperature", "Humidity", "PM10"];
    //   const node_darr = [];
    //   for (let ii in node_vars) {
    //     node_darr.push(createNodeData(node_vars[ii], timestr));
    //   }
    //   const tnode = {};
    //   tnode.Name = nodes[nextVal];
    //   tnode.UUID = nodes_uuid[nextVal];
    //   tnode.Data = node_darr;
    //   tnode.Count = count;
    //   count = count + 1;
    //   nextVal = (nextVal + 1) % nodes.length;
    //   //console.log(tnode.Name + " => " + count);
    //   socket.emit("node_data", tnode);
    // }, 10000);
  });

  //either 'io server disconnect' or 'io client disconnect'
  socket.on("disconnect", (reason) => {
    logger.info("client disconnected");
    logger.info(reason);
    ui_connected = false;
    setDataAppState(false);
    //clearInterval(interval);
    if (reason === "io server disconnect") {
      // the disconnection was initiated by the server, you need to reconnect manually
      logger.info("server disconnected the client, trying to reconnect");
      socket.connect();
    } else {
      logger.info("trying to reconnect again with server");
    }
    // else the socket will automatically try to reconnect
  });

  socket.on("error", (error) => {
    logger.info(error);
  });

  socket.on('get_cmds', (nodeName) => {
    logger.info("received get commands for node " + nodeName);
    if (dc_connected) {
      centralMasterPipeSocket.write('get_cmds:' + nodeName + "\n");
    }
    else {
      logger.info("Writing to pending get_cmds list")
      pending_get_cmds.push(nodeName);
    }
  }); 

  socket.on('ui_cmds', (cmds) => {
    console.log("received cmds from UI ");
    console.log(cmds);
    if (dc_connected) {
      console.log("writing to pipe");
      centralMasterPipeSocket.write("ui_cmds:" + JSON.stringify(cmds) + "\n");
    }
    else {
      console.log("writing to pending_ui_cmds buffer");
      addUICommandsToPendingList(cmds);
      //pending_ui_cmds.push(cmds);
      if (pending_ui_cmds.length > settings.maxNumberOfPendingData) {
        pending_ui_cmds = pending_ui_cmds.slice(pending_ui_cmds.length - settings.maxNumberOfPendingData, pending_ui_cmds.length);
      }
    }


  });

});

udpSocket.on("error", function (err) {
  logger.error("server error: " + err.stack);
  udpSocket.close();
  setTimeout(() => {
    console.log("restarting udp socket bind");
    udpSocket = dgram.createSocket();
    udpSocket.bind(settings.udp.port);
  }, 10000); // restart after 10 seconds
});

udpSocket.bind(settings.udp.port);


let nextVal = 0;
let count = 0;
const nodes = ["Node-8723af", "Node-123526", "Node-abef23"];
const nodes_uuid = [
  "4229aa95-2641-4946-8abd-2f35ee0a3de3",
  "35bf2c34-87e7-4673-9c7f-8180aae04394",
  "e78ef949-e737-4b64-8d4e-4979e65d5f21",
];

function createNodeData(name, timestr) {
  const tdat = new Object();
  var minvalue = 0;
  var maxvalue = 1;
  var tmpval;
  tdat.Name = name;
  if (name === "Temperature") {
    minvalue = 54;
    maxvalue = 70;
    tmpval = minvalue + Math.random() * (maxvalue - minvalue);
  } else if (name === "Humidity") {
    minvalue = 0.0;
    maxvalue = 25.0;
    tmpval = minvalue + Math.random() * (maxvalue - minvalue);
  } else if (name === "PM10") {
    minvalue = 5;
    maxvalue = 25;
    tmpval = Math.floor(minvalue + Math.random() * (maxvalue - minvalue));
  }
  tdat.Value = tmpval;
  tdat.Timestamp = timestr;
  return tdat;
}

establishConnection();

makeCentralMasterConnection();
