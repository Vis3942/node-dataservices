const { createLogger, format, transports } = require('winston');
require('winston-daily-rotate-file');
const fs = require("fs");

let settings_pathfile = '';
if (fs.existsSync('/home/debian/config/node_dataservices/dc_client_settings.json')) {
  settings_pathfile = '/home/debian/config/node_dataservices/dc_client_settings.json'; 
}
else {
  settings_pathfile = "./dc_client_settings.json";
}
let settings = null;
try {
  const tmpdata = fs.readFileSync(settings_pathfile);
  settings = JSON.parse(tmpdata);
}
catch (err) {
  console.log(err);
  //process.exit(1);
}

module.exports = createLogger({
    format: format.combine(
        format.timestamp({ format: "MM/DD/YYYY HH:mm:ss" }),
        format.align(),
        format.printf((i) => `${i.level}: ${[i.timestamp]}: ${i.message}`)
    ),
    transports: [
        new transports.DailyRotateFile({
            filename: settings.LogFileName,
            maxSize: '2m',
            maxFiles: '5',
            utc: true,
            extension: '.txt',
        }),
    ]
});



// var logStr = 'info';
// if (settings.LogLevel !== undefined) {
//     const tmp_settings = settings.LogLevel.toLowerCase();
//     if (tmp_settings.startsWith("info")) {
//         logStr = 'info';
//     } 
//     else if (tmp_settings.startsWith("warn")) {
//         logStr = 'warn';
//     }
//     else if (tmp_settings.startsWith('verbose')) {
//         logStr = 'verbose';
//     }
//     else if (tmp_settings.startsWith('error')) {
//         logStr = 'error';
//     }
//     else if (tmp_settings.startsWith('debug')) {
//         logStr = 'debug';
//     }
// }
