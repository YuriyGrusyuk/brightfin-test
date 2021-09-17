 

CSV File Stream To JSON
--------------

Takes a CSV file stream and parse it line by line to JSON. It accepts two callbacks: 1. Called for each file line, 2. Called at the end of the file parsing and gets passed the whole json array.
 

How to install
--------------

```batch
    npm install csv-stream-to-json --save
```

API
--------------

```batch
 parse ([Stream] inStream, [Character] separator, [Bool] toArray, [Function] rowCb, [Function] done) : Void
```

 - inStream: CSV file Readable strem
 - separator: Separator Character
 - toArray: Wheater it should return the whole document as an objects array in the done callback
 - rowCb: Callback to be executed for every parsed row. it receives the resulting object.
 - done: Callback executed at the end of the document.

How to use it
--------------


```javascript
  //Creating read stream from file uploaded to server
  var readable = req.file('avatar').read();

    csvToJson.parse(readable, ",", false, obj => {
       console.log(obj);
    }, arr => {
        console.log(arr);
    });
```








