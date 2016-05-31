import mysql from 'mysql';
import async from 'async';

var meta_data_query = `
	SELECT table_schema, table_name, column_name, ordinal_position, data_type, 
       numeric_precision, column_type, column_default, is_nullable, column_comment 
  FROM information_schema.columns 
  order by table_schema, table_name, column_name, ordinal_position;
`;

var executeQueries = function(host, port, user, password, db, queries, options, cb) {
	var connection = mysql.createConnection({
		host: host,
		user: user,
		password: password,
		database: db,
		port: port || 3306
	});
	connection.connect(function(err){
		if(err) return cb(err);

		var result = [];

		async.eachSeries(queries, function(query, callback) {
			connection.query(query, function(err, rows, fields) {
				if (err) 
					result.push(new Error(err));
				else 
					result.push(formatOutput(rows));

				return callback();
			});
		}, function() {
			connection.end();
			return cb(null, result);
		});

	});
};

var executeMetaDataQuery = function(host, port, user, password, db, options, cb){
	return this.executeQueries(host, port, user, password, db, [this.meta_data_query], options, cb);
}



var testConnection = function(host, port, user, password, db, options, cb) {
	var connection = mysql.createConnection({
		host: host,
		user: user,
		password: password,
		database: db,
		port: port || 3306
	});

	connection.connect(function(err){
		if(err) return cb(err);
		connection.end();
		return cb();
	});
};

var formatOutput = function(output) {
	var columns = Object.keys(output[0]);
	var rows = [];
	for (var i = 0; i < output.length; i++) {
		var row = [];
		for (var value in output[i]) {
			row.push(output[i][value]);
		}
		rows.push(row);
	}

	return {
		columns: columns,
		rows: rows
	};
};

module.exports = {
	executeQueries: executeQueries,
	executeMetaDataQuery: executeMetaDataQuery,
	testConnection: testConnection
};