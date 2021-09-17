const readline = require('readline');
const stream = require('stream');
const fs = require('fs');

module.exports = {
  parse: function(inStream, separator, toArray = false, rowCb, done) {
    var cant = 0;
    var fileArray = [];
    var lineObject = {};
    var properties = [];
    var values = [];
    const rl = readline.createInterface({
      input: inStream,
      output: new stream
    });

    rl.on('line', line => {
      if (cant === 0) {
        properties = line.split(separator);
      } else {
        lineObject = {};
        values = line.split(separator);
        for (var i = properties.length - 1; i >= 0; i--) {
          lineObject[properties[i]] = values[i];
        }
        if (toArray) {
          fileArray.push(lineObject);
        } else {
          rowCb(lineObject);
        }
      }
      cant++;
    });

    rl.on('close', function() {
      if (toArray) {
        done(fileArray);
      }else {
        done("END");
      }
      
    });
  }
}