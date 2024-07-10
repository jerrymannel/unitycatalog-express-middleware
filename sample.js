const cl = console.log;
const { DBSQLClient } = require('@databricks/sql');

const serverHostname = process.env.DATABRICKS_SERVER_HOSTNAME;
const httpPath = process.env.DATABRICKS_HTTP_PATH;
const token = process.env.DATABRICKS_TOKEN;

async function insert(session, table, data) {
	const queryOperation = await session.executeStatement(
		`INSERT INTO ${table} VALUES ${data}`,
		{ runAsync: true }
	);
	const result = await queryOperation.fetchAll();
	await queryOperation.close();
	return result;
}

async function select(session, table, limit) {
	const queryOperation = await session.executeStatement(
		`SELECT * FROM ${table} LIMIT ${limit}`,
		{ runAsync: true }
	);
	const result = await queryOperation.fetchAll();
	await queryOperation.close();
	return result;
}

(async () => {
	if (!token || !serverHostname || !httpPath) {
		throw new Error("Cannot find Server Hostname, HTTP Path, or " +
			"personal access token. " +
			"Check the environment variables DATABRICKS_SERVER_HOSTNAME, " +
			"DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN.");
	}

	const dbSQLClient = new DBSQLClient();
	const connectOptions = {
		token: token,
		host: serverHostname,
		path: httpPath
	};

	cl("Connecting to Databricks SQL...");
	const client = await dbSQLClient.connect(connectOptions);
	cl("Connected to Databricks SQL.");
	const session = await client.openSession();
	cl("Opened session.");

	const table = 'batman.training.department';

	let result = await select(session, table, 10);
	// console.table(result);
	cl(result);

	let data = `(${Math.floor(Math.random() * 100)},'FINANCE','EDINBURGH')`
	result = await insert(session, table, data);
	cl(result);

	result = await select(session, table, 10);
	console.table(result);

	await session.close();

})()
