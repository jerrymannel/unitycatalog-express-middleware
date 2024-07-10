"use strict"
const express = require("express")
const port = process.env.PORT || 8080

const serverHostname = process.env.DATABRICKS_SERVER_HOSTNAME;
const httpPath = process.env.DATABRICKS_HTTP_PATH;
const token = process.env.DATABRICKS_TOKEN;
const table = 'batman.training.department';

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
    "connectionDetails": {
        token: token,
        host: serverHostname,
        path: httpPath
    }
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
    // app.post("/", fooCrud.create)
    // app.put("/:id", fooCrud.update)
    // app.delete("/:id", fooCrud.deleteById)
    // app.delete("/utils/deleteMany", fooCrud.deleteMany)
    // app.post("/utils/aggregate", fooCrud.aggregate)

    app.listen(port, (err) => {
        if (!err) {
            console.log("Server started on port " + port)
        } else
            console.error(err)
    })
}

(async () => {
    await deptCRUD.connect();
    init();
})();