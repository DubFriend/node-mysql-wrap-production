# node-mysql-wrap-production

This project started from a stripped down version of
[node-mysql-wrap](https://github.com/DubFriend/node-mysql-wrap).
The intention is to reduce complexity of implementation and improve confidence
in production.

This version only accepts the poolCluster connection option, and only returns
promises (no callbacks)

A lightweight wrapper for the [node-mysql](https://github.com/felixge/node-mysql)
driver.  Providing, select, insert, update, delete, row count, and support
for promises.

`npm install mysql-wrap-production`

##Instantiation

let pool = mysql.createPool(config.mysql);
that.sql = createNodeMySQL(pool);

```javascript
var sql = createNodeMySQL(mysql.createPool({
    host: config.mysql.host,
    user: config.mysql.user,
    password: config.mysql.password,
}));
```

Pool Clusters with read write seperation is also supported
```javascript
var poolCluster = mysql.createPoolCluster({
    canRetry: true,
    removeNodeErrorCount: 1,
    restoreNodeTimeout: 20000,
    defaultSelector: 'RR'
});

poolCluster.add('MASTER', {
    connectionLimit: 200,
    host: config.mysql.host,
    port: 3306,
    user: config.mysql.user,
    password: config.mysql.password,
    database: config.mysql.database
});

poolCluster.add('SLAVE_1', {
    connectionLimit: 200,
    host: config.mysql.host,
    port: 3307,
    user: config.mysql.user,
    password: config.mysql.password,
    database: config.mysql.database
});

var sql = createNodeMySQL(poolCluster, {
    //uses the same pattern as node-mysql's getConnection patterns
    replication: {
        write: 'MASTER',
        read: 'SLAVE_*'
    }
});
```




##Methods

In general node-mysql-wrap exposes the same interface as node-mysql.  All methods
take callbacks with the same `function (err, res) {}` signature as node-mysql.
In addition all methods also return [q](https://github.com/kriskowal/q) promises.

In the following examples, parameters marked with an asterik (\*) character are
optional.

###query(sqlStatement, \*values, \*callback)
```javascript
sql.query('SELECT name FROM fruit WHERE color = "yellow"')
.then(function (res) {
    console.log(res);
    //example output: [{ name: "banana" }, { name: "lemon" }]
});
```

`query` may take a configuration object in place of the `sqlStatement` parameter.
this object allows for node-mysql's nested table join api, as well as pagination.
```javascript
sql.query({
	sql: 'SELECT * FROM fruitBasket LEFT JOIN fruit ON fruit.basketID = fruitBasket.id',
	nestTables: true,
	paginate: {
		page: 3,
		resultsPerPage: 15
	}
});
```

###one(sqlStatement, \*values, \*callback)
Works the same as sql.query except it only returns a single row instead of an array
of rows.  Adds a "LIMIT 1" clause if a LIMIT clause is not allready present in
the sqlStatement.

###select(table, \*whereEqualsObject, \*callback)
```javascript
// equivalent to sql.query('SELECT * FROM fruit WHERE color = "yellow" AND isRipe = "true"')
sql.select('fruit', { color: 'yellow', isRipe: true })
```

###selectOne(table, \*whereEqualsObject, \*callback)
Same as sql.select except selectOne returns a single row instead of an array of rows.


`select` and `selectOne` may take a configuration object in place of the table
parameter.  The configuration object add pagination and/or restrict which fields
are selected.
```javascript
sql.select({
	table: 'fruit',
	fields: ['color'],
	paginate: {
		page: 2,
		resultsPerPage: 15
	}
});
```



###insert(table, insertObject, \*callback)
```javascript
sql.insert('fruit', { name: 'plum', color: 'purple' });
```
You can also pass sql.insert an array of insertObjects to insert multiple rows in a query
```javascript
sql.insert('fruit', [
    { name: 'plum', color: 'purple'},
    { name: 'grape', color: 'green' }
])
```

###replace(table, insertObject, \*callback)
[Supports Mysql "REPLACE INTO" syntax](https://dev.mysql.com/doc/refman/5.0/en/replace.html)
```javascript
sql.replace('fruit', { uniqueKey: 5, name: 'plum', isRipe: false, color: 'brown' });
```

###save(table, insertObject, \*callback)
Inserts a new row if no duplicate unique or primary keys
are found, else it updates that row.
```sql
INSERT INTO fruit (uniqueKey, isRipe) VALUES (5, 0)
ON DUPLICATE KEY UPDATE uniqueKey=5, isRipe=0
```
```javascript
sql.save('fruit', { uniqueKey: 5, isRipe: false });
```

###update(table, setValues, \*whereEqualsObject, \*callback)
```javascript
sql.update('fruit', { isRipe: false }, { name: 'grape' })
```

###delete(table, \*whereEqualsObject, \*callback)
```javascript
sql.delete('fruit', { isRipe: false })
```
