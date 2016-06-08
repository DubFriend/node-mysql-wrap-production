'use strict';

let _ = require('lodash');
let Q = require('q');

Q.longStackSupport = true;

let createMySQLWrap = function (poolCluster, options) {
    options = options || {};

    let self = {};

    let stripLimit = function (sql) {
        return sql.replace(/ LIMIT .*/i, '');
    };

    let paginateLimit = function (fig) {
        return fig ?
            'LIMIT ' + fig.resultsPerPage + ' ' +
            'OFFSET ' + ((fig.page - 1) * fig.resultsPerPage) : '';
    };

    let getStatementObject = function (statementOrObject) {
        let statement = _.isObject(statementOrObject) ?
            statementOrObject : {
                sql: statementOrObject,
                nestTables: false
            };

        if(statement.paginate) {
            statement.sql = stripLimit(statement.sql) + ' ' +
                            paginateLimit(statement.paginate);
        }

        return statement;
    };

    let prepareWhereEquals = function (whereEquals) {
        let values = [];
        let sql = _.map(whereEquals, function (val, key) {
            values.push(key, val);
            return '?? = ?';
        }, '').join(' AND ');

        return {
            values: values,
            sql: sql ? ' WHERE ' + sql : sql
        };
    };

    let getConnection = function (readOrWrite) {
        return Q.Promise(function (resolve, reject) {
            if(options.replication) {
                poolCluster.getConnection(
                    options.replication[readOrWrite],
                    function (err, conn) {
                        if(err) {
                            reject(err);
                        }
                        else {
                            resolve(conn);
                        }
                    }
                );
            }
            else {
                poolCluster.getConnection(function (err, conn) {
                    if(err) {
                        reject(err);
                    }
                    else {
                        resolve(conn);
                    }
                });
            }
        });
    };

    let selectedFieldsSQL = function (fields) {
        return fields ? fields.join(', ') : '*';
    };

    let prepareInsertRows = function (rowOrRows) {
        let values = [];
        let fields = _.isArray(rowOrRows) ?
            _.keys(_.first(rowOrRows)) : _.keys(rowOrRows);

        // NOTE: It is important that fieldsSQL is generated before valuesSQL
        // (because the order of the values array would otherwise be incorrect)
        let fieldsSQL = '(' + _.map(fields, function (field) {
            values.push(field);
            return '??';
        }).join(', ') + ')';

        let processValuesSQL = function (row) {
            return '(' + _.map(fields, function (field) {
                values.push(row[field]);
                return '?';
            }) + ')';
        };

        let valuesSQL = _.isArray(rowOrRows) ?
            _.map(rowOrRows, processValuesSQL).join(', ') :
            processValuesSQL(rowOrRows);

        return {
            sql: fieldsSQL + ' VALUES ' + valuesSQL,
            values: values
        };
    };

    let isSQLReadOrWrite = function (statementRaw) {
        return /^SELECT/i.test(statementRaw.trim()) ? 'read' : 'write';
    };

    self.query = function (statementRaw, values) {
        let statementObject = getStatementObject(statementRaw);

        return getConnection(isSQLReadOrWrite(statementObject.sql))
        .then(function (conn) {
            return Q.Promise(function (resolve, reject) {
                conn.query(statementObject, values || [], function (err, rows) {
                    conn && conn.release && conn.release();
                    if(err) {
                        reject(err);
                    }
                    else {
                        resolve(rows);
                    }
                });
            });
        });
    };

    self.queryStream = function (statementRaw, values) {
        let statementObject = getStatementObject(statementRaw);

        return getConnection(isSQLReadOrWrite(statementObject.sql))
        .then(function (conn) {
            let stream = conn.query(statementObject, values || []).stream();

            stream.on('error', err => {
                console.error(err);
                conn && conn.release && conn.release();
            });

            stream.on('end', () => {
                conn && conn.release && conn.release();
            });

            return stream;
        });
    };

    self.one = function (statementRaw, values) {
        let statementObject = getStatementObject(statementRaw);
        statementObject.sql = stripLimit(statementObject.sql) + ' LIMIT 1';

        return self.query(statementObject, values)
        .then(function (rows) {
            return _.first(rows) || null;
        });
    };

    let buildSelect = function (tableRaw, whereEquals) {
        let statementObject = _.isObject(tableRaw) ?
            tableRaw : { table: tableRaw };
        let where = prepareWhereEquals(whereEquals);
        let values = [statementObject.table].concat(where.values);
        let sql = 'SELECT ' + selectedFieldsSQL(statementObject.fields) + ' ' +
        'FROM ?? ' + where.sql + (
            statementObject.paginate ?
                ' ' + paginateLimit(statementObject.paginate) : ''
        );

        return { sql: sql, values: values };
    };

    self.select = function (tableRaw, whereEquals) {
        let query = buildSelect(tableRaw, whereEquals);
        return self.query(query.sql, query.values);
    };

    self.selectStream = function (tableRaw, whereEquals) {
        let query = buildSelect(tableRaw, whereEquals);
        return self.queryStream(query.sql, query.values);
    };

    self.selectOne = function (tableRaw, whereEquals) {
        let statementObject = _.isObject(tableRaw) ?
            tableRaw : { table: tableRaw };
        let where = prepareWhereEquals(whereEquals);
        let values = [statementObject.table].concat(where.values);

        return self.one(
            'SELECT ' + selectedFieldsSQL(statementObject.fields) +
            ' FROM ?? ' + where.sql,
            values
        );
    };

    self.insert = function (table, rowOrRows) {
        let rows = prepareInsertRows(rowOrRows);
        return self.query(
            'INSERT INTO ?? ' + rows.sql,
            [table].concat(rows.values)
        );
    };

    self.replace = function (table, rowRaw, callback) {
        let row = prepareInsertRows(rowRaw);
        return self.query(
            'REPLACE INTO ?? ' + row.sql,
            [table].concat(row.values)
        );
    };

    self.save = function (table, rowOrRows) {
        let rows = _.isArray(rowOrRows) ? rowOrRows : [rowOrRows];

        let prepareSaveRows = function () {
            let insertRow = prepareInsertRows(rows);
            let setValues = [];

            let setSQL = _.map(_.first(rows), function (val, key) {
                setValues.push(key, key);
                return '?? = VALUES(??)';
            }).join(', ');

            return {
                sql: insertRow.sql + ' ON DUPLICATE KEY UPDATE ' + setSQL,
                values: insertRow.values.concat(setValues)
            };
        };

        let row = prepareSaveRows();

        return self.query(
            'INSERT INTO ?? ' + row.sql,
            [table].concat(row.values)
        );
    };

    self.update = function (table, setData, whereEquals) {
        let prepareSetRows = function (setData) {
            let values = [];
            let sql = ' SET ' + _.map(setData, function (val, key) {
                values.push(key, val);
                return '?? = ?';
            }).join(', ');
            return { values: values, sql: sql };
        };

        let set = prepareSetRows(setData);
        let where = prepareWhereEquals(whereEquals);
        let values = [table].concat(set.values).concat(where.values);
        return self.query('UPDATE ??' + set.sql + where.sql, values);
    };

    self.delete = function (table, whereEquals) {
        let where = prepareWhereEquals(whereEquals);
        let values = [table].concat(where.values);
        return self.query('DELETE FROM ?? ' + where.sql, values);
    };

    return self;
};

module.exports = createMySQLWrap;
