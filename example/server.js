"use strict"
const { DBSQLClient } = require('@databricks/sql');
const express = require("express");
const port = process.env.PORT || 18080;

const serverHostname = process.env.DATABRICKS_SERVER_HOSTNAME;
const httpPath = process.env.DATABRICKS_HTTP_PATH;
const token = process.env.DATABRICKS_TOKEN;
const table = 'batman.training.department';

const connectionOptions = {
    token: token,
    host: serverHostname,
    path: httpPath
};

const UCCRUD = require("../app");

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
    app.get("/utils/count", async (req, res) => {
        try {
            let result = await deptCRUD.count(req.query);
            res.json(result);
        } catch (err) {
            res.status(500).send(err.message);
        }
    });
    app.post("/", async (req, res) => {
        try {
            let result = await deptCRUD.create(req.body);
            res.json(result);
        } catch (err) {
            res.status(500).send(err.message);
        }
    });
    app.put("/:id", async (req, res) => {
        try {
            let result = await deptCRUD.update(req.params.id, req.body, req.query);
            res.json(result);
        } catch (err) {
            res.status(500).send(err.message);
        }
    });
    app.delete("/:id", async (req, res) => {
        try {
            let result = await deptCRUD.delete(req.params.id, req.query);
            res.json(result);
        } catch (err) {
            res.status(500).send(err.message);
        }
    });
    app.delete("/utils/deleteMany", async (req, res) => {
        try {
            let result = await deptCRUD.deleteMany(req.body, req.query);
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