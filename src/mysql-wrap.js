'use strict';

const _ = require('lodash');
const Q = require('q');
const squel = require('squel');

Q.longStackSupport = true;

const createMySQLWrap = (poolCluster, options) => {
    options = options || {};

    let self = {};

    const stripLimit = sql => sql.replace(/ LIMIT .*/i, '');

    const paginateLimit = fig => fig ?
        'LIMIT ' + fig.resultsPerPage + ' ' +
        'OFFSET ' + ((fig.page - 1) * fig.resultsPerPage) : '';

    const addCalcFoundRows = sql => {
        const pieces = sql.split(' ');
        pieces.splice(1, 0, 'SQL_CALC_FOUND_ROWS');
        return pieces.join(' ');
    };

    const getStatementObject = statementOrObject => {
        const statement = _.isObject(statementOrObject) ?
            statementOrObject : {
                sql: statementOrObject,
                nestTables: false
            };

        if(statement.paginate) {
            statement.sql = addCalcFoundRows(
                stripLimit(statement.sql) + ' ' +
                paginateLimit(statement.paginate)
            );
        }
        else if(statement.resultCount) {
            statement.sql = addCalcFoundRows(statement.sql);
        }

        return statement;
    };

    const prepareWhereEquals = whereEquals => {
        const values = [];
        const sql = _.map(whereEquals, (val, key) => {
            values.push(key, val);
            return '?? = ?';
        }, '').join(' AND ');

        return {
            values: values,
            sql: sql ? ' WHERE ' + sql : sql
        };
    };

    const getConnection = readOrWrite => Q.Promise((resolve, reject) => {
        if(options.replication) {
            poolCluster.getConnection(
                options.replication[readOrWrite],
                (err, conn) => err ? reject(err) : resolve(conn)
            );
        }
        else {
            poolCluster.getConnection(
                (err, conn) => err ? reject(err) : resolve(conn)
            );
        }
    });

    const selectedFieldsSQL = fields => fields ? fields.join(', ') : '*';

    const prepareInsertRows = rowOrRows => {
        const values = [];
        const fields = _.isArray(rowOrRows) ?
            _.keys(_.first(rowOrRows)) : _.keys(rowOrRows);

        // NOTE: It is important that fieldsSQL is generated before valuesSQL
        // (because the order of the values array would otherwise be incorrect)
        const fieldsSQL = '(' + _.map(fields, field => {
            values.push(field);
            return '??';
        }).join(', ') + ')';

        const processValuesSQL = row => '(' + _.map(fields, field => {
            values.push(row[field]);
            return '?';
        }) + ')';

        const valuesSQL = _.isArray(rowOrRows) ?
            _.map(rowOrRows, processValuesSQL).join(', ') :
            processValuesSQL(rowOrRows);

        return {
            sql: fieldsSQL + ' VALUES ' + valuesSQL,
            values: values
        };
    };

    const isSQLReadOrWrite = statementRaw => /^SELECT/i.test(statementRaw.trim()) ?
        'read' : 'write';

    self.build = () => {
        const wrap = method => () => {
            const s = squel[method]();

            s.run = fig => {
                const p = s.toParam();
                return self.query(
                    _.extend({ sql: p.text }, fig || {}),
                    p.values
                );
            };

            s.one = fig => {
                const p = s.toParam();
                return self.one(
                    _.extend({ sql: p.text }, fig || {}),
                    p.values
                );
            };

            return s;
        };

        let buildSelf = {
            select: wrap('select'),
            update: wrap('update'),
            delete: wrap('delete'),
            insert: wrap('insert')
        };

        return buildSelf;
    };

    self.query = (statementRaw, values) => {
        const statementObject = getStatementObject(statementRaw);

        return getConnection(isSQLReadOrWrite(statementObject.sql))
        .then((conn) => Q.Promise((resolve, reject) => {
            conn.query(statementObject, values || [], (err, rows) => {
                if(err) {
                    conn.release();
                    reject(err);
                }
                else if (statementObject.paginate || statementObject.resultCount) {
                    conn.query('SELECT FOUND_ROWS() AS count', (err, result) => {
                        conn.release();
                        if(err) {
                            reject(err);
                        }
                        else if(statementObject.paginate){
                            resolve({
                                resultCount: _.first(result).count,
                                pageCount: Math.ceil(
                                    _.first(result).count /
                                    statementObject.paginate.resultsPerPage
                                ),
                                currentPage: statementObject.paginate.page,
                                results: rows
                            });
                        }
                        else if(statementObject.resultCount) {
                            resolve({
                                resultCount: _.first(result).count,
                                results: rows
                            });
                        }
                    });
                }
                else {
                    conn.release();
                    resolve(rows);
                }
            });
        }));
    };

    self.queryStream = (statementRaw, values) => {
        const statementObject = getStatementObject(statementRaw);

        return getConnection(isSQLReadOrWrite(statementObject.sql))
        .then(conn => {
            const stream = conn.query(statementObject, values || []).stream();

            stream.on('error', err => {
                console.error(err);
                conn && conn.release && conn.release();
            });

            stream.on('end', () => conn && conn.release && conn.release());

            return stream;
        });
    };

    self.one = (statementRaw, values) => {
        const statementObject = getStatementObject(statementRaw);
        statementObject.sql = stripLimit(statementObject.sql) + ' LIMIT 1';

        return self.query(statementObject, values)
        .then(rows => _.first(rows) || null);
    };

    const buildSelect = (tableRaw, whereEquals) => {
        const statementObject = _.isObject(tableRaw) ?
            tableRaw : { table: tableRaw };
        const where = prepareWhereEquals(whereEquals);
        const values = [statementObject.table].concat(where.values);
        const sql = 'SELECT ' + selectedFieldsSQL(statementObject.fields) + ' ' +
        'FROM ?? ' + where.sql + (
            statementObject.paginate ?
                ' ' + paginateLimit(statementObject.paginate) : ''
        );

        return { sql: sql, values: values };
    };

    self.select = (tableRaw, whereEquals) => {
        const query = buildSelect(tableRaw, whereEquals);
        return self.query(query.sql, query.values);
    };

    self.selectStream = (tableRaw, whereEquals) => {
        const query = buildSelect(tableRaw, whereEquals);
        return self.queryStream(query.sql, query.values);
    };

    self.selectOne = (tableRaw, whereEquals) => {
        const statementObject = _.isObject(tableRaw) ?
            tableRaw : { table: tableRaw };
        const where = prepareWhereEquals(whereEquals);
        const values = [statementObject.table].concat(where.values);

        return self.one(
            'SELECT ' + selectedFieldsSQL(statementObject.fields) +
            ' FROM ?? ' + where.sql,
            values
        );
    };

    self.insert = (table, rowOrRows) => {
        const rows = prepareInsertRows(rowOrRows);
        return self.query(
            'INSERT INTO ?? ' + rows.sql,
            [table].concat(rows.values)
        );
    };

    self.replace = (table, rowRaw, callback) => {
        const row = prepareInsertRows(rowRaw);
        return self.query(
            'REPLACE INTO ?? ' + row.sql,
            [table].concat(row.values)
        );
    };

    self.save = (table, rowOrRows) => {
        const rows = _.isArray(rowOrRows) ? rowOrRows : [rowOrRows];

        const prepareSaveRows = () => {
            const insertRow = prepareInsertRows(rows);
            const setValues = [];

            const setSQL = _.map(_.first(rows), (val, key) => {
                setValues.push(key, key);
                return '?? = VALUES(??)';
            }).join(', ');

            return {
                sql: insertRow.sql + ' ON DUPLICATE KEY UPDATE ' + setSQL,
                values: insertRow.values.concat(setValues)
            };
        };

        const row = prepareSaveRows();

        return self.query(
            'INSERT INTO ?? ' + row.sql,
            [table].concat(row.values)
        );
    };

    self.update = (table, setData, whereEquals) => {
        const prepareSetRows = setData => {
            const values = [];
            const sql = ' SET ' + _.map(setData, (val, key) => {
                values.push(key, val);
                return '?? = ?';
            }).join(', ');
            return { values: values, sql: sql };
        };

        const set = prepareSetRows(setData);
        const where = prepareWhereEquals(whereEquals);
        const values = [table].concat(set.values).concat(where.values);
        return self.query('UPDATE ??' + set.sql + where.sql, values);
    };

    self.delete = (table, whereEquals) => {
        const where = prepareWhereEquals(whereEquals);
        const values = [table].concat(where.values);
        return self.query('DELETE FROM ?? ' + where.sql, values);
    };

    self.escape = data => poolCluster.escape(data);
    self.escapeId = data => poolCluster.escapeId(data);

    return self;
};

module.exports = createMySQLWrap;
