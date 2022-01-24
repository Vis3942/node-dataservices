const fs = require('fs');
const dc_client_settings = require('./dc_client_settings.json');
const logLocation = dc_client_settings.logLocation;

function RotateFilesByDate(mostRecentCount, prefix, extension) {
    fs.readdir( logLocation, (err, files) => {
        if (err) {
            console.log(err);
            return;
        }
        let tmpfiles = files.filter(x => x.startsWith(prefix) && x.endsWith("." + extension))
        tmpfiles = tmpfiles.sort();
        if (tmpfiles.length > mostRecentCount) {
            for (let ii=0; ii < tmpfiles.length - mostRecentCount; ii++) {
                let tmpDelName = (logLocation.endsWith("/") ? logLocation : logLocation + "/") + tmpfiles[ii];
                fs.unlink(tmpDelName, (err) => {
                    if (err) {
                        console.log(err);
                        return;
                    }
                    console.log('deleted file ' + tmpDelName);
                })
            }
        }
    })
}

module.exports = { RotateFilesByDate };
