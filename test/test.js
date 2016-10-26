'use strict';

const _ = require('lodash');
const Q = require('q');
const chai = require('chai');
const config = require('../config');
const mysql = require('mysql');
const createNodeMySQL = require('../src/mysql-wrap');

describe('mysqlWrap', () => {
    before(done => {
        this.stripIds = data => _.isArray(data) ?
            _.map(data, this.stripIds) : _.omit(data, 'id');

        let pool = mysql.createPool(config.mysql);
        this.sql = createNodeMySQL(pool);
        pool.getConnection((err, conn) => {
            this.masterConn = conn;
            done();
        });
    });

    beforeEach(done => {
        this.masterConn.query('TRUNCATE TABLE `table`', (err, res) => {
            this.a = { id: 1, unique: 'a', field: 'foo' };
            this.b = { id: 2, unique: 'b', field: 'bar' };
            this.c = { id: 3, unique: 'c', field: 'foo' };
            this.masterConn.query(
                'INSERT INTO `table` (`unique`, `field`) ' +
                'VALUES ' + _.map([this.a, this.b, this.c], row => {
                    return '("' + row.unique + '", "' + row.field + '")';
                }).join(', '),
                (err, res) => {
                    this.masterConn.query(
                        'TRUNCATE TABLE `table2`',
                        (err, res) => {
                            this.masterConn.query(
                                'INSERT INTO `table2` (`field`) ' +
                                'VALUES ("bar")',
                                (err) => {
                                    done();
                                }
                            );
                        }
                    );
                }
            );
        });
    });

    describe('build', () => {
        it('should return query generator', done => {
            this.sql.build().select().from('`table`')
            .where('field = ?', this.b.field).run()
            .then(resp => {
                chai.assert.deepEqual(resp, [this.b]);
                done();
            }).done();
        });

        it('should be able to pass query options through "run" command', done => {
            this.sql.build().select().from('`table`')
            .where('id = ?', this.b.id).run({ resultCount: true })
            .then(resp => {
                chai.assert.deepEqual(
                    resp,
                    {
                        resultCount: 1,
                        results: [this.b]
                    }
                );
                done();
            }).done();
        });

        it('should be invokable through a "one" command', done => {
            this.sql.build().select().from('`table`')
            .where('id = ?', this.b.id).one()
            .then(resp => {
                chai.assert.deepEqual(resp, this.b);
                done();
            }).done();
        });
    });

    describe('queryStream', () => {
        it('should return a readable stream of rows', done => {
            let expected = [this.a, this.b, this.c];

            this.sql.queryStream('SELECT * FROM `table` ORDER BY `id`')
            .then(stream => {
                stream.on('data', row => {
                    chai.assert.deepEqual(row, expected.shift());
                });

                stream.on('end', () => done());
            }).done();
        });
    });

    describe('selectStream', () => {
        it('should return a readable stream of rows', done => {
            this.sql.selectStream('table', { id: this.a.id })
            .then(stream => {
                stream.on('data', row => {
                    chai.assert.deepEqual(row, this.a);
                });

                stream.on('end', () => done());
            }).done();
        });
    });

    describe('query', () => {
        it('should select without values array', done => {
            this.sql.query('SELECT * FROM `table`')
            .then((rows) => {
                chai.assert.sameDeepMembers(rows, [this.a, this.b, this.c]);
                done();
            }).done();
        });

        it('should have variable parameters using "?"', done => {
            this.sql.query('SELECT * FROM `table` WHERE id = ?', [2])
            .then(rows => {
                chai.assert.deepEqual(rows, [this.b]);
                done();
            }).done();
        });

        it('should have table/field parameters using "??"', done => {
            this.sql.query('SELECT ?? FROM `table`', ['unique'])
            .then(rows => {
                chai.assert.sameDeepMembers(rows, [
                    { unique: 'a' },
                    { unique: 'b' },
                    { unique: 'c' }
                ]);
                done();
            }).done();
        });

        it('should propogate stack trace to application code', done => {
            this.sql.query('SELECT wrong FROM `table`')
            .catch(err => {
                chai.assert.ok(/test\.js/.test(err.stack));
                done();
            }).done();
        });

        it('should be case insensitive', done => {
            this.sql.query('sElEcT * FRoM `table` Where id = ?', [3])
            .then(rows => {
                chai.assert.deepEqual(rows, [this.c]);
                done();
            }).done();
        });

        it('should insert', done => {
            this.sql.query(
                'INSERT INTO `table` (`unique`, `field`) ' +
                'VALUES ("testUniqueValue", "testFieldValue")'
            )
            .then(res => {
                chai.assert.strictEqual(res.affectedRows, 1, 'affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'insertId');
                this.masterConn.query(
                    'SELECT * FROM `table` WHERE id = 4',
                    (err, rows) => {
                        chai.assert.deepEqual(rows, [{
                            id: 4,
                            unique: 'testUniqueValue',
                            field: 'testFieldValue'
                        }]);
                        done();
                    }
                );
            }).done();
        });

        it('should update', done => {
            this.sql.query(
                'UPDATE `table` SET `field` = "edit" ' +
                'WHERE `field` = "foo"'
            )
            .then(res => {
                chai.assert.strictEqual(res.affectedRows, 2, 'affectedRows');
                chai.assert.strictEqual(res.changedRows, 2, 'changedRows');
                this.masterConn.query(
                    'SELECT * FROM `table` WHERE `field` = "edit"',
                    (err, rows) => {
                        chai.assert.sameDeepMembers(rows, [
                            { id: 1, unique: 'a', field: 'edit' },
                            { id: 3, unique: 'c', field: 'edit' }
                        ], 'fields updated in database');
                        done();
                    }
                );
            }).done();
        });

        it('should delete', done => {
            this.sql.query('DELETE FROM `table` WHERE `field` = "foo"')
            .then(res => {
                chai.assert.strictEqual(res.affectedRows, 2, 'affectedRows');
                this.masterConn.query(
                    'SELECT * FROM `table` WHERE `field` = "foo"',
                    (err, rows) => {
                        chai.assert.deepEqual(rows, [], 'fields deleted');
                        done();
                    }
                );
            }).done();
        });

        it('should have option to nest join', done => {
            this.sql.query({
                sql: 'SELECT * FROM `table` ' +
                     'INNER JOIN `table2` ' +
                     'ON `table`.`field` = `table2`.`field`',
                nestTables: true
            })
            .then(rows => {
                chai.assert.deepEqual(rows, [{
                    table: {
                        id: 2,
                        unique: "b",
                        field: "bar"
                    },
                    table2: {
                        id: 1,
                        field: "bar"
                    }
                }]);
                done();
            }).done();
        });

        it('should have option to paginate', done => {
            this.sql.query({
                sql: 'SELECT * FROM `table`',
                paginate: {
                    page: 1,
                    resultsPerPage: 2
                }
            })
            .then(resp => {
                chai.assert.deepEqual(_.omit(resp, 'results'), {
                    resultCount: 3,
                    pageCount: 2,
                    currentPage: 1
                });
                chai.assert.sameDeepMembers(resp.results, [this.a, this.b]);
                done();
            }).done();
        });

        it('should have option to include result count', done => {
            this.sql.query({
                sql: 'SELECT * FROM `table` LIMIT 2',
                resultCount: true
            })
            .then(resp => {
                chai.assert.deepEqual(_.omit(resp, 'results'), {
                    resultCount: 3
                });
                chai.assert.sameDeepMembers(resp.results, [this.a, this.b]);
                done();
            }).done();
        });
    });

    describe('one', () => {
        it('should select a single row', done => {
            this.sql.one('SELECT * FROM `table` WHERE id = 1')
            .then(row => {
                chai.assert.deepEqual(row, this.a);
                done();
            }).done();
        });
    });

    describe('select', () => {
        it('should select by table and basic where clause', done => {
            this.sql.select('table', { id: 3, field: 'foo' })
            .then(rows => {
                chai.assert.deepEqual(rows, [this.c]);
                done();
            }).done();
        });

        it('should have option to paginate', done => {
            this.sql.select({
                table: 'table',
                paginate: {
                    page: 1,
                    resultsPerPage: 2
                }
            })
            .then(rows => {
                chai.assert.deepEqual(rows, [this.a , this.b]);
                done();
            }).done();
        });

        it('should have option to select field', done => {
            this.sql.select({ table: 'table', fields: ['id'] })
            .then(rows => {
                chai.assert.deepEqual(rows, [{ id: 1 }, { id: 2 }, { id: 3 }]);
                done();
            }).done();
        });
    });

    describe('selectOne', () => {
        it('should select single row by table and basic where clause', done => {
            this.sql.selectOne('table', { field: 'foo' })
            .then(row => {
                chai.assert.deepEqual(row, this.a);
                done();
            }).done();
        });

        it('should have option to select fields', done => {
            this.sql.selectOne({ table: 'table', fields: ['id'] })
            .then(row => {
                chai.assert.deepEqual(row, { id: 1 });
                done();
            }).done();
        });
    });

    describe('insert', () => {
        it('should insert a single row', done => {
            this.sql.insert('table', { unique: 'd', field: 'baz' })
            .then(res => {
                chai.assert.strictEqual(res.affectedRows, 1, 'affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'insertId');
                this.masterConn.query(
                    'SELECT * FROM `table` WHERE `id` = 4',
                    (err, rows) => {
                        chai.assert.deepEqual(
                            rows,
                            [{ id: 4, unique: 'd', field: 'baz' }],
                            'inserts into database'
                        );
                        done();
                    }
                );
            }).done();
        });

        it('should insert multiple rows', done => {
            this.sql.insert('table', [
                { unique: 'd', field: 'new' },
                { unique: 'e', field: 'new' }
            ])
            .then(res => {
                chai.assert.strictEqual(res.affectedRows, 2, 'affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'insertId');
                this.masterConn.query(
                    'SELECT * FROM `table` WHERE `field` = "new"',
                    (err, rows) => {
                        chai.assert.deepEqual(rows, [
                            { id: 4, unique: 'd', field: 'new' },
                            { id: 5, unique: 'e', field: 'new' }
                        ], 'inserts into database');
                        done();
                    }
                );
            }).done();
        });
    });

    describe('replace', () => {
        it('should insert row', done => {
            this.sql.replace('table', { unique: 'd', field: 'baz' })
            .then(res => {
                chai.assert.strictEqual(res.affectedRows, 1, 'affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'insertId');
                this.masterConn.query(
                    'SELECT * FROM `table` WHERE `id` = 4',
                    (err, res) => {
                        chai.assert.deepEqual(res, [
                            { id: 4, unique: 'd', field: 'baz' }
                        ], 'inserts into database');
                        done();
                    }
                );
            }).done();
        });

        it('should replace row with same unique key', done => {
            this.sql.replace('table', { unique: 'c', field: 'replaced' })
            .then(() => {
                this.masterConn.query(
                    'SELECT * FROM `table` WHERE `unique` = "c"',
                    (err, res) => {
                        chai.assert.deepEqual(
                            res,
                            [{ id: 4, unique: 'c', field: 'replaced' }],
                            'replaces existing row and increments id'
                        );
                        done();
                    }
                );
            }).done();
        });
    });

    describe('save', () => {
        it('should insert row if does not exist', done => {
            this.sql.save('table', { unique: 'd', field: 'baz' })
            .then(res => {
                chai.assert.strictEqual(res.affectedRows, 1, 'returns affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'returns insert id');
                this.masterConn.query(
                    'SELECT * FROM `table` WHERE `id` = 4',
                    (err, res) => {
                        chai.assert.deepEqual(
                            res,
                            [{ id: 4, unique: 'd', field: 'baz' }]
                        );
                        done();
                    }
                );
            }).done();
        });

        it('should update row if exists by unique constraint', done => {
            this.sql.save('table', { unique: 'c', field: 'update' })
            .then(() => {
                this.masterConn.query(
                    'SELECT * FROM `table` WHERE `unique` = "c"',
                    (err, res) => {
                        chai.assert.deepEqual(
                            res,
                            [{ id: 3, unique: 'c', field: 'update' }]
                        );
                        done();
                    }
                );
            }).done();
        });

        it('should handle bulk save', done => {
            let rows = [
                { unique: 'a', field: 'edit-a' },
                { unique: 'b', field: 'edit-b' },
                { unique: 'd', field: 'new-field' }
            ];
            this.sql.save('table', rows)
            .then(() => {
                this.masterConn.query(
                    'SELECT * FROM `table`',
                    (err, res) => {
                        chai.assert.sameDeepMembers(
                            this.stripIds(res),
                            this.stripIds(rows.concat([this.c]))
                        );
                        done();
                    }
                );
            }).done();
        });
    });

    describe('update', () => {
        it('should update row', done => {
            this.sql.update('table', { field: 'edit', unique: 'd' }, { id: 1 })
            .then(res => {
                this.masterConn.query(
                    'SELECT * FROM `table`',
                    (err, res) => {
                        chai.assert.deepEqual(res, [
                            { id: 1, unique: 'd', field: 'edit' },
                            { id: 2, unique: 'b', field: 'bar' },
                            { id: 3, unique: 'c', field: 'foo' }
                        ], 'updates database');
                        done();
                    }
                );
            }).done();
        });
    });

    describe('delete', () => {
        it('should delete rows by where equals config', done => {
            this.sql.delete('table', { field: 'foo' })
            .then(res => {
                this.masterConn.query(
                    'SELECT * FROM `table`',
                    (err, res) => {
                        chai.assert.deepEqual(res, [this.b]);
                        done();
                    }
                );
            }).done();
        });
    });
});
