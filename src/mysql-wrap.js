'use strict';

const _ = require('lodash');
const Q = require('q');
const squel = require('squel');

Q.longStackSupport = true;

const createMySQLWrap = (poolCluster, options, connection) => {
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
        if(connection) {
            resolve(connection);
        }
        else {
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

    const runCursor = (q, fig) => {
        const orderBy = _.map(
            _.isArray(fig.orderBy) ? fig.orderBy : [fig.orderBy],
            o =>  _.isString(o) ?
                { field: o, isAscending: true } :
                _.extend(
                    _.omit(_.clone(o), 'direction'),
                    { isAscending: o.direction === 'DESC' ? false : true }
                )
        );

        const isAscending = fig.last && !fig.first ? false : true;
        const delimeter = '#';

        const decodeCursor = c => _.map(
            new Buffer(c, 'base64')
            .toString('ascii').split(delimeter),
            (v, i) => orderBy[i].deserialize ? orderBy[i].deserialize(v) : v
        );

        const encodeCursor = r => new Buffer(_.map(
            orderBy,
            o => o.serialize ? o.serialize(r[o.field]) : String(r[o.field])
        ).join(delimeter)).toString('base64');

        const buildWhereArgs = (values, isGreaterThan) => {
            const build = (values, orderBy, isGreaterThan) => {
                const sql = _.map(
                    orderBy,
                    (o, i) => i === values.length - 1 ?
                        `${o.field} ${
                            (o.isAscending ? isGreaterThan : !isGreaterThan) ?
                                '>' : '<'
                        } ?` :
                        `${o.field} = ?`
                ).join(' AND ');

                let sqls = [sql];
                let mappedValues = [values];

                if(values.length > 1) {
                    const w = build(
                        _.initial(values),
                        _.initial(orderBy),
                        isGreaterThan
                    );
                    sqls = sqls.concat(w.sqls);
                    mappedValues = mappedValues.concat(w.mappedValues);
                }

                return {
                    sqls: sqls,
                    mappedValues: mappedValues
                };
            };

            const w = build(values, orderBy, isGreaterThan);

            return [w.sqls.reverse().join(' OR ')]
                .concat(_.flatten(w.mappedValues.reverse()));
        };

        _.each(orderBy, o => {
            q.order(
                o.field,
                o.isAscending ? isAscending : !isAscending
            );
        });

        if(fig.after) {
            q.where.apply(q, buildWhereArgs(decodeCursor(fig.after), true));
        }

        if(fig.before) {
            q.where.apply(q, buildWhereArgs(decodeCursor(fig.before), false));
        }

        q.limit(isAscending ? fig.first : fig.last);

        const query = q.toParam();

        return self.query({
            sql: query.text,
            resultCount: true
        }, query.values)
        .then(resp => {
            if(isAscending && fig.last && fig.last < resp.results.length) {
                resp.results = resp.results.slice(
                    resp.results.length - fig.last,
                    resp.results.length
                );
            }
            else if(!isAscending && fig.last && fig.last < resp.results.length) {
                resp.results = resp.results.slice(0, fig.last);
            }

            if(!isAscending) {
                resp.results = resp.results.reverse();
            }

            return resp;
        })
        .then(resp => ({
            resultCount: resp.resultCount,
            pageInfo: {
                hasPreviousPage: fig.last ? resp.resultCount > fig.last : false,
                hasNextPage: fig.first ? resp.resultCount > fig.first : false
            },
            edges: _.map(
                resp.results,
                r => ({ node: r, cursor: encodeCursor(r) })
            )
        }));
    };

    self.build = () => {
        const wrap = method => () => {
            const s = squel[method]();


            s.run = fig => {
                fig = fig || {};
                if(fig.cursor) {
                    return runCursor(s, fig.cursor);
                }
                else {
                    const p = s.toParam();
                    return self.query(
                        _.extend({ sql: p.text }, fig),
                        p.values
                    );
                }
            };

            s.one = fig => {
                const p = s.toParam();
                return self.one(
                    _.extend({ sql: p.text }, fig || {}),
                    p.values
                );
            };

            s.whereIfDefined = (sql, value) => {
                if(value !== undefined) {
                    s.where(sql, value);
                }
                return s;
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

    self.connection = () => getConnection('write')
    .then(conn => {
        let sql = createMySQLWrap(null, options, conn);
        sql.release = () => conn && conn.release && conn.release();
        return sql;
    });

    // self.connection = () => Q(self);

    const finishedWithConnection = conn => {
        if(!connection) {
            conn && conn.release && conn.release();
        }
    };

    self.query = (statementRaw, values) => {
        const statementObject = getStatementObject(statementRaw);

        return getConnection(isSQLReadOrWrite(statementObject.sql))
        .then(conn => Q.Promise((resolve, reject) => {
            conn.query(statementObject, values || [], (err, rows) => {
                if(err) {
                    finishedWithConnection(conn);
                    reject(err);
                }
                else if (
                    statementObject.paginate ||
                    statementObject.resultCount
                ) {
                    conn.query('SELECT FOUND_ROWS() AS count', (err, result) => {
                        finishedWithConnection(conn);
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
                    finishedWithConnection(conn);
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
                finishedWithConnection(conn);
            });

            stream.on('end', () => finishedWithConnection(conn));

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
