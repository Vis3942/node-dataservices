const fs = require('fs');
const dc_client_settings = require('./dc_client_settings.json');
const lineByLine = require('n-readlines');
const logLocation = dc_client_settings.logLocation;
const csv_headers = {}

function GetHeaderLine(pathFileName) {
    // sync function
    const liner = new lineByLine(pathFileName);
    let line = "";
    let lineBuf = liner.next();
    if (lineBuf !== false) {
        line = lineBuf.toString().trim();
    }
    if (liner.fd !== null) {
        liner.close();
    }
    return line;
}

function WriteToCsvFile(node_data) {
    let currDate = new Date();
    let datestr = currDate.getUTCFullYear().toString() + "_" + 
        (currDate.getUTCMonth()+1).toString().padStart(2, "0") + "_" +    
        (currDate.getUTCDate()).toString().padStart(2, "0");
    let fname = (logLocation.endsWith("/") ? logLocation : logLocation + "/") + node_data.Name + "_" + dc_client_settings.logPrefix + "_" + datestr + ".csv";
    if (!fs.existsSync(fname)) {
        // create csv file header from node_data
        let hdrs = [];
        let tmpd = [];
        // Timestamp from first element for all elements 
        hdrs.push("Timestamp");
        tmpd.push(node_data.Data[0].Timestamp);
        for (let ii in node_data.Data) {
            hdrs.push(node_data.Data[ii].Name);
            tmpd.push(node_data.Data[ii].Value.toString());
        }
        csv_headers[node_data.Name] = hdrs;
        try {
            fs.writeFileSync(fname, [hdrs.join(',') + "\r\n",  tmpd.join(',') + "\r\n"].join(''));
        }   
        catch(e) {
            console.log("Error creating file " + fname);
            console.log("data from node " + node_data.Name + " is lost");
        }     
    }
    else 
    {
        // app relaunched, csv_headers variable maybe empty, (and file exists) if yes, load from file
        if (csv_headers[node_data.Name] === undefined) {
            let hdrLine = GetHeaderLine(fname);
            if (hdrLine.length > 0) {
                csv_headers[node_data.Name] = hdrLine.split(',');
            }
        }
        // in case something does not work
        if (csv_headers[node_data.Name] !== undefined) {
            //console.log(csv_headers[node_data.Name]);
            let tmparr = Array.apply(null, Array(csv_headers[node_data.Name].length) );
            tmparr = tmparr.map((x,i) => {
                return '';
            } );
            let kk = csv_headers[node_data.Name].indexOf("Timestamp");
            //console.log("Timestamp index = " + kk);
            if ( kk >= 0 ) {
                tmparr[kk] = node_data.Data[0].Timestamp;
            }
            else {
                console.log("Node: " + node_data.Name + " failed to write variable Timestamp to csv file");
                console.log("hdrs = " + csv_headers[node_data.Name]);
            }
            for (let jj in node_data.Data) {
                kk = csv_headers[node_data.Name].indexOf(node_data.Data[jj].Name);
                if (kk >= 0) {
                    tmparr[kk] = node_data.Data[jj].Value.toString();        
                }
                else {
                    console.log("Node: " + node_data.Name + " failed to write variable " + node_data.Data[jj].Name + " to csv file");
                    console.log("hdrs = " + csv_headers[node_data.Name]);
                }
            }
            try {
                fs.appendFileSync(fname, tmparr.join(',') + "\r\n");
            }
            catch(e) {
                console.log("Error updating file " + fname);
                console.log("data from node " + node_data.Name + " is not appended to data file.");
            }     
        }
        else {
            console.log(csv_headers);
            console.log("Headers for node : " + node_data.Name + " is not defined. Failed write to csv file" );
        }
    }
}

module.exports.WriteToCsvFile = WriteToCsvFile;