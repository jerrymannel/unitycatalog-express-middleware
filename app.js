const { DBSQLClient } = require('@databricks/sql');
const pino = require('pino');
const _ = require('lodash');

const version = require('./package.json').version;

global.logger = pino({
	level: process.env.LOG_LEVEL || 'info',
	formatters: {
		level: (label) => {
			return {
				level: label.toUpperCase(),
				name: `UnityCatalogCRUD v${version}`
			};
		}
	},
	timestamp: pino.stdTimeFunctions.isoTime,
});

let logger = global.logger;

const utils = require('./utils');

function UnityCatalogCRUD(options) {
	logger.debug('UnityCatalogCRUD constructor called');
	logger.trace(`Options :: ${JSON.stringify(options)}`);
	this.table = options.table;
	this.fields = options.fields;
	this.session = null;
	this.defaultField = options.defaultField;
	this.connectionOptions = {
		token: options.connectionDetails.token,
		host: options.connectionDetails.host,
		path: options.connectionDetails.path
	};
}

UnityCatalogCRUD.prototype.connect = async function () {
	try {
		logger.debug('Connecting to Databricks SQL...');
		const dbSQLClient = new DBSQLClient();
		const client = await dbSQLClient.connect(this.connectionOptions);
		logger.debug('Connected to Databricks SQL.');
		this.session = await client.openSession();
		logger.debug('Session opened.');
	} catch (err) {
		logger.error(`Error while connecting to Databricks SQL :: ${err}`);
		throw err;
	}
}

UnityCatalogCRUD.prototype.list = async function (options) {
	try {
		logger.debug('list(): Listing rows in DB.');
		logger.trace(`list(): Filters for listing :: ${JSON.stringify(options)}`);

		let whereClause;
		if (options?.filter && !_.isEmpty(options.filter)) {
			whereClause = utils.whereClause(options?.filter);
		}
		logger.trace(`list(): Where clause :: ${whereClause}`);

		const selectClause = utils.selectClause(this.fields, options?.select) || '*';
		const limitClause = utils.limitClause(options?.count, options?.page);
		const orderByClause = utils.orderByClause(this.fields, this.defaultField, options?.sort);

		let sql = `SELECT ${selectClause} FROM ${this.table}`;
		if (whereClause) {
			sql += whereClause;
		}
		if (orderByClause) {
			sql += orderByClause;
		}
		if (limitClause) {
			sql += limitClause;
		}

		logger.trace(`list(): Generated SQL Query :: ${sql}`);

		const queryOperation = await this.session.executeStatement(sql, { runAsync: true });
		const result = await queryOperation.fetchAll();
		await queryOperation.close();
		logger.debug(`list(): List query successful`);
		return result;
	} catch (err) {
		logger.error(`list(): Error while listing rows in DB :: ${err}`);
		throw err;
	}
}

UnityCatalogCRUD.prototype.fetchOne = async function (value, options) {
	try {
		logger.debug(`fetchOne(): Finding row with value ${value}`);
		logger.trace(`fetchOne(): Options :: ${JSON.stringify(options)}`);

		if (!options.fieldName) {
			logger.trace(`fetchOne(): Field name not provided. Using default field name`);
			options.fieldName = this.defaultField;
		}

		const selectClause = utils.selectClause(this.fields, options?.select) || '*';
		let sql = `SELECT ${selectClause} FROM ${this.table} WHERE ${options.fieldName}='${value}'`;
		logger.trace(`fetchOne(): Generated SQL Query :: ${sql}`);

		const queryOperation = await this.session.executeStatement(sql, { runAsync: true });
		const result = await queryOperation.fetchAll();
		await queryOperation.close();
		logger.debug(`fetchOne(): List query successful`);
		if (result.length === 0) return null;
		return result[0];
	} catch (err) {
		logger.error(`fetchOne(): Error while fetching row with value ${value} :: ${err}`);
		throw err;
	}
}

UnityCatalogCRUD.prototype.count = async function (options) {
	try {
		logger.debug(`count(): Counting rows in DB`);
		logger.trace(`count(): Options :: ${JSON.stringify(options)}`);

		let whereClause;
		if (options?.filter && !_.isEmpty(options.filter)) {
			whereClause = utils.whereClause(options?.filter);
		}
		logger.trace(`count(): Where clause :: ${whereClause}`);

		let sql = `SELECT COUNT(*) as count FROM ${this.table}`;
		if (whereClause) {
			sql += whereClause;
		}
		logger.trace(`count(): Generated SQL Query :: ${sql}`);

		const queryOperation = await this.session.executeStatement(sql, { runAsync: true });
		const result = await queryOperation.fetchAll();
		await queryOperation.close();
		logger.debug(`count(): Count query successful`);
		return result[0];
	} catch (err) {
		logger.error(`count(): Error while counting rows in DB :: ${err}`);
		throw err;
	}
}

UnityCatalogCRUD.prototype.create = async function (data) {
	try {
		logger.debug(`create(): Creating new row in DB`);
		logger.trace(`create(): Data to create :: ${JSON.stringify(data)}`);

		if (!data) {
			logger.error(`create(): No data to insert`);
			throw new Error('No data to insert');
		}
		if (!Array.isArray(data)) {
			logger.trace(`create(): Data is not an array. Converting to array`);
			data = [data];
		}

		const stmt = utils.insertManyStatement(this.fields, data);
		if (!stmt) {
			logger.error(`create(): No data to insert`);
			throw new Error('No data to insert');
		}
		let sql = `INSERT INTO ${this.table} ${stmt}`;
		logger.trace(`create(): SQL statement for insert :: ${sql}`);

		const queryOperation = await this.session.executeStatement(sql, { runAsync: true });
		const result = await queryOperation.fetchAll();
		await queryOperation.close();
		return result[0];
	} catch (err) {
		logger.error(`create(): Error while creating new row in DB :: ${err}`);
		throw err;
	}
}

UnityCatalogCRUD.prototype.update = async function (value, data, options) {
	try {
		logger.debug(`update(): Updating row with value ${value}`);
		logger.trace(`update(): Data to update :: ${JSON.stringify(data)}`);
		logger.trace(`update(): Options :: ${JSON.stringify(options)}`);

		if (!options.fieldName) {
			logger.trace(`update(): Field name not provided. Using default field name`);
			options.fieldName = this.defaultField;
		}

		const stmt = utils.updateStatement(this.fields, data);
		if (!stmt) {
			logger.error(`update(): No data to update`);
			throw new Error('No data to update');
		}

		let sql = `UPDATE ${this.table} ${stmt} WHERE ${options.fieldName}='${value}'`;
		logger.trace(`update(): SQL statement for update :: ${sql}`);

		const queryOperation = await this.session.executeStatement(sql, { runAsync: true });
		const result = await queryOperation.fetchAll();
		await queryOperation.close();
		return result[0];
	} catch (err) {
		logger.error(`update(): Error while updating row with value ${value} :: ${err}`);
		throw err;
	}
}

UnityCatalogCRUD.prototype.delete = async function (value, options) {
	try {
		logger.debug(`delete(): Deleting row with value ${value}`);
		logger.trace(`delete(): Options :: ${JSON.stringify(options)}`);

		if (!options.fieldName) {
			logger.trace(`delete(): Field name not provided. Using default field name`);
			options.fieldName = this.defaultField;
		}

		let sql = `DELETE FROM ${this.table} WHERE ${options.fieldName}='${value}'`;
		logger.trace(`delete(): SQL statement for delete :: ${sql}`);

		const queryOperation = await this.session.executeStatement(sql, { runAsync: true });
		const result = await queryOperation.fetchAll();
		await queryOperation.close();
		return result[0];
	} catch (err) {
		logger.error(`delete(): Error while deleting row with value ${value} :: ${err}`);
		throw err;
	}
}

UnityCatalogCRUD.prototype.deleteMany = async function (values, options) {
	try {
		logger.debug(`deleteMany(): Deleting rows with values ${values}`);
		logger.trace(`deleteMany(): Options :: ${JSON.stringify(options)}`);

		if (!options.fieldName) {
			logger.trace(`deleteMany(): Field name not provided. Using default field name`);
			options.fieldName = this.defaultField;
		}

		let sql = `DELETE FROM ${this.table} WHERE ${options.fieldName} IN (${values.map(value => `'${value}'`).join(',')})`;
		logger.trace(`deleteMany(): SQL statement for delete :: ${sql}`);

		const queryOperation = await this.session.executeStatement(sql, { runAsync: true });
		const result = await queryOperation.fetchAll();
		await queryOperation.close();
		return result[0];
	} catch (err) {
		logger.error(`deleteMany(): Error while deleting rows with values ${values} :: ${err}`);
		throw err;
	}
}

module.exports = UnityCatalogCRUD;