# unitycatalog-express-middleware
databricks Unity Catalog express middleware

# Installation

```sh
npm i unitycatalog-express-middleware
```

# Usage

```js
"use strict"
const { DBSQLClient } = require('@databricks/sql');
const express = require("express");
const port = process.env.PORT || 8080

const serverHostname = process.env.DATABRICKS_SERVER_HOSTNAME;
const httpPath = process.env.DATABRICKS_HTTP_PATH;
const token = process.env.DATABRICKS_TOKEN;
const table = 'mytable.training.department';

const connectionOptions = {
    token: token,
    host: serverHostname,
    path: httpPath
};

const UCCRUD = require("unitycatalog-express-middleware");

// Define the table structure
const columns = {
    "deptcode": "int",
    "deptname": "string",
    "location": "string"
}

const options = {
    "table": table,
    "fields": columns,
    "defaultField": "deptcode",
    "session": null,
}

var deptCRUD = new UCCRUD(options);

function init() {
    var app = express()
    app.use(express.json())

    app.use((req, res, next) => {
        console.log(req.method, req.url)
        next()
    });

    app.get("/", async (req, res) => {
        try {
            let result = await deptCRUD.list(req.query);
            res.json(result);
        } catch (err) {
            res.status(500).send(err.message);
        }
    });
    app.get("/:id", async (req, res) => {
        try {
            let result = await deptCRUD.fetchOne(req.params.id, req.query);
            if (!result) return res.status(404).send("Not found");
            res.json(result);
        } catch (err) {
            res.status(500).send(err.message);
        }
    });

    app.listen(port, (err) => {
        if (!err) {
            console.log("Server started on port " + port)
        } else
            console.error(err)
    })
}

async function connect() {
    try {
        console.log('Connecting to Databricks SQL...');
        const dbSQLClient = new DBSQLClient();
        const client = await dbSQLClient.connect(connectionOptions);
        console.log('Connected to Databricks SQL.');
        // Get the session and assign it to the UCCRUD session
        deptCRUD.session = await client.openSession();
        console.log('Session opened.');
    } catch (err) {
        console.log(`Error while connecting to Databricks SQL :: ${err}`);
        throw err;
    }
}

(async () => {
    await connect();
    init();
})();
```

Full working code available under [example]("./example/server.js").

# Method documentation

## list(options)
> TDB

## fetchOne(value, options)
> TDB

## count(options)
> TDB

## create(data)
> TDB

## update(value, data, options)
> TDB

## delete(value, options)
> TDB

## deleteMany(values, options)
> TDB


# Release notes

## 1.0.1

* Removed logic of establishing connection from UCCRUD. This will let you share the same connection instance with multiple UCCRUD instances.

## 1.0.0

* First release 
* Working APIs middleware for
  * Fetch
  * Fetch one
  * Create
  * Update one
  * Update many
  * Delete one
  * Delete many