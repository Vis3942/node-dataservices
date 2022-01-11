const io = require("socket.io-client");
const moment = require("moment");
const dgram = require("dgram");
var udpSocket = dgram.createSocket("udp4");
//const settings = require("./dc_client_settings.json");
const process =require('process');
const { send } = require("process");
const fs = require("fs");
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
  process.exit(1);
}

const logger = require('./dc_client_logger')
const { establishConnection, endConnection, setDataAppState } = require('./dc_client_pipestatus');

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
  retrying = false;
}

function dataEventHandler(data) {
  if (got_json) {
    recv_data = data;
    //console.log("creating recv_data with ");
    //console.log(recv_data);
    //console.log("-------------");
  } else {
    //console.log("appending data");
    recv_data = recv_data + data;
    //console.log(recv_data);
    //console.log(" xxxxxxxxxxxxxxxxx    ");
  }
  got_json = testJSON(recv_data);
  //console.log("got_json = " + got_json);
  if (got_json) {
    const obj = JSON.parse(recv_data);
    const sendObj = formatDataForUI(obj);
    //console.log(sendObj);
    recv_data = '';
    if (sendObj !== null)
    {
      if ( (ui_connected) && (socket !== null) ) {
        socket.emit('node_data', sendObj);
        logger.info("Sending to ui " + sendObj.Data.length + " items from " + sendObj.Name);
      } 
      else {
        logger.info("Got data for UI (Not sent) " + sendObj.Data.length + " items from " + sendObj.Name);
      }
    }
  } 
  // else {
  //   console.log(recv_data);
  // }
}

function errorEventHandler(err) {
  logger.error(err);
  //console.log("reconnecting... to cm pipe");
}

function closeEventHandler() {
  if (!retrying) {
    retrying = true;
    logger.info('reconnecting... to central master');
  }
  setTimeout(makeCentralMasterConnection, settings.PipeConnectionTimeoutSecs*1000);
}

let centralMasterPipeSocket = net.Socket();
centralMasterPipeSocket.on('connect', connectEventHandler);
centralMasterPipeSocket.on('data', dataEventHandler);
centralMasterPipeSocket.on('error', errorEventHandler);
centralMasterPipeSocket.on('close', closeEventHandler);

// var centralMasterPipeClient = net.connect("/tmp/CoreFxPipe_PLSDCConnection", function () {
//   console.log("connected to server");
// });

var ui_connected = false;

function testJSON(text) {
  if (typeof text !== "string") {
    return false;
  }
  try {
    JSON.parse(text);
    return true;
  } catch (error) {
    return false;
  }
}

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
    logger.error(error);
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
