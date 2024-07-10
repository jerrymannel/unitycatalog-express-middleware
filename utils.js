const parser = require('where-in-json');
const _ = require('lodash');

let logger = global.logger;

function whereClause(filter) {
	try {
		logger.trace(`whereClause(): filter : ${JSON.stringify(filter)}`);
		if (!filter || _.isEmpty(filter)) {
			return null;
		}
		if (typeof filter === 'string') {
			logger.trace(`whereClause(): filter is string. Parsing to JSON.`);
			filter = JSON.parse(filter);
		}

		let generatedWhereClause = ' WHERE ' + parser.toWhereClause(filter);
		logger.trace(`whereClause(): generatedWhereClause : ${generatedWhereClause}`);

		return generatedWhereClause;
	} catch (err) {
		logger.error(`whereClause(): Error while generating where clause :: ${err}`);
		throw err;
	}
}

function selectClause(fields, select) {
	logger.trace(`selectClause(): select : ${select}`);
	if (!select) {
		logger.trace(`selectClause(): select is null. Returning null.`);
		return null;
	}
	const cols = select.split(',');
	const keys = [];
	cols.forEach(dataKey => {
		if (fields[dataKey]) keys.push(dataKey);
	});
	logger.trace(`selectClause(): keys : ${keys.join(", ")}`);
	if (keys.length > 0) {
		return keys.join(', ');
	}
	return null;
}

function limitClause(count, page) {
	logger.trace(`limitClause(): count : ${count}, page : ${page}`);
	if (count == -1) {
		logger.trace(`limitClause(): count is -1. Returning null.`);
		return null;
	}
	if (!count) {
		count = 30;
	}
	if (!page) {
		page = 1;
	}
	let generatedLimitClause = ` LIMIT ${count} OFFSET ${(page - 1) * count}`;
	logger.trace(`limitClause(): generatedLimitClause : ${generatedLimitClause}`);
	return generatedLimitClause;
}

function orderByClause(fields, defaultField, sort) {
	logger.trace(`orderByClause(): sort : ${sort}`);
	sort = sort || defaultField;
	if (!sort) {
		logger.trace(`orderByClause(): sort is null. Returning null.`);
		return null;
	}
	logger.trace(`orderByClause(): sort : ${sort}`);
	const cols = sort.split(',');
	const orderBy = [];
	cols.forEach(dataKey => {
		if (dataKey.startsWith('-')) {
			dataKey = dataKey.substring(1);
			if (fields[dataKey]) orderBy.push(`${dataKey} DESC`);
		} else {
			if (fields[dataKey]) orderBy.push(`${dataKey} ASC`);
		}
	});
	if (orderBy.length > 0) {
		let generatedOrderByClause = ' ORDER BY ' + orderBy.join(', ');
		logger.trace(`orderByClause(): generatedOrderByClause : ${generatedOrderByClause}`);
		return generatedOrderByClause
	}
	logger.trace(`orderByClause(): Returning null.`);
	return null;
}

const STRING_TYPES = ['string', 'char', 'varchar', 'text'];

function insertManyStatement(fields, data) {
	logger.trace(`insertManyStatement(): fields : ${JSON.stringify(fields)}, data : ${JSON.stringify(data)}`);
	const cols = Object.keys(fields);
	let values = [];
	const valuesList = [];
	data.forEach(obj => {
		cols.forEach(item => {
			const type = fields[item].toLowerCase();
			const val = obj[item];
			if (val === undefined || val === null) {
				values.push('NULL');
			} else if (STRING_TYPES.includes(type)) {
				values.push(`'${val}'`);
			} else {
				values.push(val);
			}
		});
		valuesList.push(values.join(', '));
		values = [];
	})

	logger.trace(`insertManyStatement(): valuesList : ${valuesList}`);
	if (valuesList.length > 0) {
		const generatedInsertManyStatement = `(${cols.join(', ')}) VALUES (${valuesList.join('), (')})`;
		logger.trace(`insertManyStatement(): generatedInsertManyStatement : ${generatedInsertManyStatement}`);
		return generatedInsertManyStatement;
	}
	logger.trace(`insertManyStatement(): Returning null.`);
	return null;
}

function updateStatement(fields, data) {
	logger.trace(`updateStatement(): fields : ${JSON.stringify(fields)}, data : ${JSON.stringify(data)}`);
	const sets = [];
	Object.keys(data).forEach(dataKey => {
		const type = fields[dataKey].toLowerCase();
		if (type) {
			if (STRING_TYPES.includes(type)) {
				sets.push(`${dataKey}='${data[dataKey]}'`);
			} else {
				sets.push(`${dataKey}=${data[dataKey]}`);
			}
		}
	});
	if (sets.length > 0) {
		const generatedUpdateStatement = 'SET ' + sets.join(', ');
		logger.trace(`updateStatement(): generatedUpdateStatement : ${generatedUpdateStatement}`);
		return generatedUpdateStatement;
	}
	logger.trace(`updateStatement(): Returning null.`);
	return null;
}


module.exports = {
	whereClause,
	selectClause,
	limitClause,
	orderByClause,
	insertManyStatement,
	updateStatement
};