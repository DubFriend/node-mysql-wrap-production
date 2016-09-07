'use strict';

const _ = require('lodash');
const Q = require('q');
const chai = require('chai');
const config = require('../config');
const mysql = require('mysql');
const createNodeMySQL = require('../src/mysql-wrap');

describe('mysqlWrap', function () {
    before(function (done) {
        let that = this;

        that.stripIds = data => _.isArray(data) ?
            _.map(data, that.stripIds) : _.omit(data, 'id');

        let pool = mysql.createPool(config.mysql);
        that.sql = createNodeMySQL(pool);
        pool.getConnection(function (err, conn) {
            that.masterConn = conn;
            done();
        });
    });

    beforeEach(function (done) {
        let that = this;
        that.masterConn.query('TRUNCATE TABLE `table`', function (err, res) {
            that.a = { id: 1, unique: 'a', field: 'foo' };
            that.b = { id: 2, unique: 'b', field: 'bar' };
            that.c = { id: 3, unique: 'c', field: 'foo' };
            that.masterConn.query(
                'INSERT INTO `table` (`unique`, `field`) ' +
                'VALUES ' + _.map([that.a, that.b, that.c], function (row) {
                    return '("' + row.unique + '", "' + row.field + '")';
                }).join(', '),
                function (err, res) {
                    that.masterConn.query(
                        'TRUNCATE TABLE `table2`',
                        function (err, res) {
                            that.masterConn.query(
                                'INSERT INTO `table2` (`field`) ' +
                                'VALUES ("bar")',
                                function (err) {
                                    done();
                                }
                            );
                        }
                    );
                }
            );
        });
    });

    describe('build', function () {
        it('should return query generator', function (done) {
            this.sql.build().select().from('table')
            .where('field = ?', this.b.field).run()
            .then(resp => {
                chai.assert.deepEqual(resp, [this.b]);
                done();
            }).done();
        });

        it('should be able to pass query options through "run" command', function (done) {
            this.sql.build().select().from('table')
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

        it('should be invokable through a "one" command', function (done) {
            this.sql.build().select().from('table')
            .where('id = ?', this.b.id).one()
            .then(resp => {
                chai.assert.deepEqual(resp, this.b);
                done();
            }).done();
        });
    });

    describe('queryStream', function () {
        it('should return a readable stream of rows', function (done) {
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

    describe('selectStream', function () {
        it('should return a readable stream of rows', function (done) {
            this.sql.selectStream('table', { id: this.a.id })
            .then(stream => {
                stream.on('data', row => {
                    chai.assert.deepEqual(row, this.a);
                });

                stream.on('end', () => done());
            }).done();
        });
    });

    describe('query', function () {
        it('should select without values array', function (done) {
            let that = this;
            that.sql.query('SELECT * FROM `table`')
            .then(function (rows) {
                chai.assert.sameDeepMembers(rows, [that.a, that.b, that.c]);
                done();
            })
            .done();
        });

        it('should have variable parameters using "?"', function (done) {
            let that = this;
            that.sql.query('SELECT * FROM `table` WHERE id = ?', [2])
            .then(function (rows) {
                chai.assert.deepEqual(rows, [that.b]);
                done();
            })
            .done();
        });

        it('should have table/field parameters using "??"', function (done) {
            let that = this;
            that.sql.query('SELECT ?? FROM `table`', ['unique'])
            .then(function (rows) {
                chai.assert.sameDeepMembers(rows, [
                    { unique: 'a' },
                    { unique: 'b' },
                    { unique: 'c' }
                ]);
                done();
            })
            .done();
        });

        it('should propogate stack trace to application code', function (done) {
            let that = this;
            that.sql.query('SELECT wrong FROM `table`')
            .catch(function (err) {
                chai.assert.ok(/test\.js/.test(err.stack));
                done();
            }).done();
        });

        it('should be case insensitive', function (done) {
            let that = this;
            that.sql.query('sElEcT * FRoM `table` Where id = ?', [3])
            .then(function (rows) {
                chai.assert.deepEqual(rows, [that.c]);
                done();
            })
            .done();
        });

        it('should insert', function (done) {
            let that = this;
            that.sql.query(
                'INSERT INTO `table` (`unique`, `field`) ' +
                'VALUES ("testUniqueValue", "testFieldValue")'
            )
            .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 1, 'affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'insertId');
                that.masterConn.query(
                    'SELECT * FROM `table` WHERE id = 4',
                    function (err, rows) {
                        chai.assert.deepEqual(rows, [{
                            id: 4,
                            unique: 'testUniqueValue',
                            field: 'testFieldValue'
                        }]);
                        done();
                    }
                );
            })
            .done();
        });

        it('should update', function (done) {
            let that = this;
            that.sql.query(
                'UPDATE `table` SET `field` = "edit" ' +
                'WHERE `field` = "foo"'
            )
            .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 2, 'affectedRows');
                chai.assert.strictEqual(res.changedRows, 2, 'changedRows');
                that.masterConn.query(
                    'SELECT * FROM `table` WHERE `field` = "edit"',
                    function (err, rows) {
                        chai.assert.sameDeepMembers(rows, [
                            { id: 1, unique: 'a', field: 'edit' },
                            { id: 3, unique: 'c', field: 'edit' }
                        ], 'fields updated in database');
                        done();
                    }
                );
            })
            .done();
        });

        it('should delete', function (done) {
            let that = this;
            that.sql.query('DELETE FROM `table` WHERE `field` = "foo"')
            .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 2, 'affectedRows');
                that.masterConn.query(
                    'SELECT * FROM `table` WHERE `field` = "foo"',
                    function (err, rows) {
                        chai.assert.deepEqual(rows, [], 'fields deleted');
                        done();
                    }
                );
            })
            .done();
        });

        it('should have option to nest join', function (done) {
            let that = this;
            that.sql.query({
                sql: 'SELECT * FROM `table` ' +
                     'INNER JOIN `table2` ' +
                     'ON `table`.`field` = `table2`.`field`',
                nestTables: true
            })
            .then(function (rows) {
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
            })
            .done();
        });

        it('should have option to paginate', function (done) {
            let that = this;
            that.sql.query({
                sql: 'SELECT * FROM `table`',
                paginate: {
                    page: 1,
                    resultsPerPage: 2
                }
            })
            .then(function (resp) {
                chai.assert.deepEqual(_.omit(resp, 'results'), {
                    resultCount: 3,
                    pageCount: 2,
                    currentPage: 1
                });
                chai.assert.sameDeepMembers(resp.results, [that.a, that.b]);
                done();
            })
            .done();
        });

        it('should have option to include result count', function (done) {
            let that = this;
            that.sql.query({
                sql: 'SELECT * FROM `table` LIMIT 2',
                resultCount: true
            })
            .then(function (resp) {
                chai.assert.deepEqual(_.omit(resp, 'results'), {
                    resultCount: 3
                });
                chai.assert.sameDeepMembers(resp.results, [that.a, that.b]);
                done();
            })
            .done();
        });
    });

    describe('one', function () {
        it('should select a single row', function (done) {
            let that = this;
            that.sql.one('SELECT * FROM `table` WHERE id = 1')
            .then(function (row) {
                chai.assert.deepEqual(row, that.a);
                done();
            })
            .done();
        });
    });

    describe('select', function () {
        it('should select by table and basic where clause', function (done) {
            let that = this;
            that.sql.select('table', { id: 3, field: 'foo' })
            .then(function (rows) {
                chai.assert.deepEqual(rows, [that.c]);
                done();
            })
            .done();
        });

        it('should have option to paginate', function (done) {
            let that = this;
            that.sql.select({
                table: 'table',
                paginate: {
                    page: 1,
                    resultsPerPage: 2
                }
            })
            .then(function (rows) {
                chai.assert.deepEqual(rows, [that.a , that.b]);
                done();
            })
            .done();
        });

        it('should have option to select field', function (done) {
            let that = this;
            that.sql.select({ table: 'table', fields: ['id'] })
            .then(function (rows) {
                chai.assert.deepEqual(rows, [{ id: 1 }, { id: 2 }, { id: 3 }]);
                done();
            })
            .done();
        });
    });

    describe('selectOne', function () {
        it('should select single row by table and basic where clause', function (done) {
            let that = this;
            that.sql.selectOne('table', { field: 'foo' })
            .then(function (row) {
                chai.assert.deepEqual(row, that.a);
                done();
            })
            .done();
        });

        it('should have option to select fields', function (done) {
            let that = this;
            that.sql.selectOne({ table: 'table', fields: ['id'] })
            .then(function (row) {
                chai.assert.deepEqual(row, { id: 1 });
                done();
            })
            .done();
        });
    });

    describe('insert', function () {
        it('should insert a single row', function (done) {
            let that = this;
            that.sql.insert('table', { unique: 'd', field: 'baz' })
            .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 1, 'affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'insertId');
                that.masterConn.query(
                    'SELECT * FROM `table` WHERE `id` = 4',
                    function (err, rows) {
                        chai.assert.deepEqual(
                            rows,
                            [{ id: 4, unique: 'd', field: 'baz' }],
                            'inserts into database'
                        );
                        done();
                    }
                );
            })
            .done();
        });

        it('should insert multiple rows', function (done) {
            let that = this;
            that.sql.insert('table', [
                { unique: 'd', field: 'new' },
                { unique: 'e', field: 'new' }
            ])
            .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 2, 'affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'insertId');
                that.masterConn.query(
                    'SELECT * FROM `table` WHERE `field` = "new"',
                    function (err, rows) {
                        chai.assert.deepEqual(rows, [
                            { id: 4, unique: 'd', field: 'new' },
                            { id: 5, unique: 'e', field: 'new' }
                        ], 'inserts into database');
                        done();
                    }
                );
            })
            .done();
        });
    });

    describe('replace', function () {
        it('should insert row', function (done) {
            let that = this;
            that.sql.replace('table', { unique: 'd', field: 'baz' })
            .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 1, 'affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'insertId');
                that.masterConn.query(
                    'SELECT * FROM `table` WHERE `id` = 4',
                    function (err, res) {
                        chai.assert.deepEqual(res, [
                            { id: 4, unique: 'd', field: 'baz' }
                        ], 'inserts into database');
                        done();
                    }
                );
            })
            .done();
        });

        it('should replace row with same unique key', function (done) {
            let that = this;
            that.sql.replace('table', { unique: 'c', field: 'replaced' })
            .then(function () {
                that.masterConn.query(
                    'SELECT * FROM `table` WHERE `unique` = "c"',
                    function (err, res) {
                        chai.assert.deepEqual(
                            res,
                            [{ id: 4, unique: 'c', field: 'replaced' }],
                            'replaces existing row and increments id'
                        );
                        done();
                    }
                );
            })
            .done();
        });
    });

    describe('save', function () {
        it('should insert row if does not exist', function (done) {
            let that = this;
            that.sql.save('table', { unique: 'd', field: 'baz' })
            .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 1, 'returns affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'returns insert id');
                that.masterConn.query(
                    'SELECT * FROM `table` WHERE `id` = 4',
                    function (err, res) {
                        chai.assert.deepEqual(
                            res,
                            [{ id: 4, unique: 'd', field: 'baz' }]
                        );
                        done();
                    }
                );
            })
            .done();
        });

        it('should update row if exists by unique constraint', function (done) {
            let that = this;
            that.sql.save('table', { unique: 'c', field: 'update' })
            .then(function () {
                that.masterConn.query(
                    'SELECT * FROM `table` WHERE `unique` = "c"',
                    function (err, res) {
                        chai.assert.deepEqual(
                            res,
                            [{ id: 3, unique: 'c', field: 'update' }]
                        );
                        done();
                    }
                );
            })
            .done();
        });

        it('should handle bulk save', function (done) {
            let that = this;
            let rows = [
                { unique: 'a', field: 'edit-a' },
                { unique: 'b', field: 'edit-b' },
                { unique: 'd', field: 'new-field' }
            ];
            that.sql.save('table', rows)
            .then(function () {
                that.masterConn.query(
                    'SELECT * FROM `table`',
                    function (err, res) {
                        chai.assert.sameDeepMembers(
                            that.stripIds(res),
                            that.stripIds(rows.concat([that.c]))
                        );
                        done();
                    }
                );
            })
            .done();
        });
    });

    describe('update', function () {
        it('should update row', function (done) {
            let that = this;
            that.sql.update('table', { field: 'edit', unique: 'd' }, { id: 1 })
            .then(function (res) {
                that.masterConn.query(
                    'SELECT * FROM `table`',
                    function (err, res) {
                        chai.assert.deepEqual(res, [
                            { id: 1, unique: 'd', field: 'edit' },
                            { id: 2, unique: 'b', field: 'bar' },
                            { id: 3, unique: 'c', field: 'foo' }
                        ], 'updates database');
                        done();
                    }
                );
            })
            .done();
        });
    });

    describe('delete', function () {
        it('should delete rows by where equals config', function (done) {
            let that = this;
            that.sql.delete('table', { field: 'foo' })
            .then(function (res) {
                that.masterConn.query(
                    'SELECT * FROM `table`',
                    function (err, res) {
                        chai.assert.deepEqual(res, [that.b]);
                        done();
                    }
                );
            })
            .done();
        });
    });
});
