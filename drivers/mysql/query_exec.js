var ERROR = require("../QueryExecError");
var WrapperPromise = require("../WrapperPromise");

// ****************************************************************************
// QueryBuilder "Query Execution" methods.
// -----
// @param    Object    qb            The QueryBuilder object
// @param    Object    adapter        The connection adapter object
// ****************************************************************************
const QueryExec = function (qb, conn) {

    const exec = (sql, callback) => {
        if (Object.prototype.toString.call(conn) == Object.prototype.toString.call({})) {
            conn.query(sql, (err, results) => {
                // Standardize some important properties
                if (!err && results.length > 0) {

                    // Insert ID
                    if (results.hasOwnProperty('insertId')) {
                        results.insert_id = results.insertId;
                    }

                    // Affected Rows
                    if (results.hasOwnProperty('affectedRows')) {
                        results.affected_rows = results.affectedRows;
                    }

                    // Changed Rows
                    if (results.hasOwnProperty('changedRows')) {
                        results.changed_rows = results.changedRows;
                    }
                }
                callback(err, results);

            });
        } else {
            throw ERROR.NO_CONN_OBJ_ERR;
        }
    };

    return {
        query: function (sql, callback) {
            if (typeof callback !== "function") return new WrapperPromise(sql, exec).promisify();
            exec(sql, callback);
        },

        count: function (table, callback) {
            if (typeof table === 'function' && typeof callback !== 'function') {
                table = null;
                callback = table;
            }

            const sql = qb.count(table);
            qb.reset_query(sql);

            /**
             * 
             * @param {Object} err - Error object
             * @param {Object} row - Results of rows
             * 
             * @description
             * here a handler is a callback that is called after the query has been executed
             * If this is called from wrapper pomise, We must our self  handle the resolve and
             * reject section from this handler
             */
            const handler = function (err, row) {
                if (!err) {
                    //console.dir(row[0].numrows);
                    if (typeof callback !== "function") { this.resolve(row[0].numrows); return; }
                    callback(err, row[0].numrows);
                }
                else {
                    if (typeof callback !== "function") { this.reject(err); return; }
                    callback(err, row);
                }
            }

            if (typeof callback !== "function") return new WrapperPromise(sql, exec, handler).promisify();
            exec(sql, handler);
        },

        get: function (table, callback, conn) {
            // The table parameter is optional, it could be the callback...
            if (typeof table === 'function' && typeof callback !== 'function') {
                callback = table;
            }
            // else if (typeof table === 'undefined' && typeof callback !== 'function') {
            //     throw new Error("No callback function has been provided in your 'get' call!");
            // }

            const sql = qb.get(table);
            qb.reset_query(sql);
            if (typeof callback !== "function") return new WrapperPromise(sql, exec).promisify();
            exec(sql, callback);
        },

        get_where: function (table, where, callback) {
            if (typeof table !== 'string' && !Array.isArray(table)) {
                throw ERROR.FIRST_PARAM_OF_GET_WHERE_ERR;
            }
            if (Object.prototype.toString.call(where) !== Object.prototype.toString.call({})) {
                throw ERROR.SECOND_PARAM_OF_GET_WHERE_ERR;
            }
            const sql = qb.get_where(table, where);
            qb.reset_query(sql);
            if (typeof callback !== "function") return new WrapperPromise(sql, exec).promisify();
            exec(sql, callback);
        },

        insert: function (table, set, callback, ignore, suffix) {
            if (typeof table !== "string") throw ERROR.NO_TBL_NME_ERRl;
            const sql = qb.insert(table, set, ignore, suffix);
            qb.reset_query(sql);
            if (typeof callback !== "function") return new WrapperPromise(sql, exec).promisify();
            exec(sql, callback);
        },

        insert_ignore: function (table, set, on_dupe, callback) {
            if (typeof table !== "string") throw ERROR.NO_TBL_NME_ERRl;
            if (typeof on_dupe === 'function') {
                callback = on_dupe;
                on_dupe = null;
            }
            const sql = qb.insert_ignore(table, set, on_dupe);
            qb.reset_query(sql);
            if (typeof callback !== "function") return new WrapperPromise(sql, exec).promisify();
            exec(sql, callback);
        },

        insert_batch: function (table, set, callback) {
            if (typeof table !== "string") throw ERROR.NO_TBL_NME_ERRl;
            const sql = qb.insert_batch(table, set);
            qb.reset_query(sql);
            if (typeof callback !== "function") return new WrapperPromise(sql, exec).promisify();
            exec(sql, callback);
        },

        update: function (table, set, where, callback) {

            if (typeof table !== "string") throw ERROR.NO_TBL_NME_ERRl;
            // The where parameter is optional, it could be the callback...
            if (typeof where === 'function' && typeof callback !== 'function') {
                callback = where;
                where = null;
            }
            else if (typeof where === 'undefined') {
                throw ERROR.NO_WHERE_CLUASE_ERR
            }
            else if (typeof where === 'undefined' || where === false || (where !== null && typeof where === 'object' && where.length == 0)) {
                where = null;
            }

            const sql = qb.update(table, set, where);
            qb.reset_query(sql);
            if (typeof callback !== "function") return new WrapperPromise(sql, exec).promisify();
            exec(sql, callback);
        },

        // TODO: Write this complicated-ass function
        update_batch: function (table, set, index, where, callback) {
            if (typeof table !== "string") throw ERROR.NO_TBL_NME_ERRl;
            // The where parameter is optional, it could be the callback...
            if (typeof where === 'function' && typeof callback !== 'function') {
                callback = where;
                where = null;
            }
            // else if (typeof where === 'undefined' && typeof callback !== 'function') {
            //     throw new Error("No callback function has been provided in your update_batch call!");
            // }
            else if (typeof where === 'undefined') {
                throw ERROR.NO_WHERE_CLUASE_ERR
            }
            else if (typeof where === 'undefined' || where === false || (where !== null && typeof where === 'object' && where.length == 0)) {
                where = null;
            }

            const sqls = qb.update_batch(table, set, index, where);
            const results = null;
            const errors = [];

            // Execute each batch of (at least) 100
            (function next_batch() {
                const sql = sqls.shift();
                qb.reset_query(sql);

                /**
                 * Look here more care fully
                 */
                exec(sql, (err, res) => {
                    if (!err) {
                        if (null === results) {
                            results = res;
                        } else {
                            results.affected_rows += res.affected_rows;
                            results.changed_rows += res.changed_rows;
                        }
                    } else {
                        errors.push(err);
                    }

                    if (sqls.length > 0) {
                        setTimeout(next_batch, 0);
                    } else {
                        return callback(errors, results);
                    }
                });
            })();
        },

        delete: function (table, where, callback) {
            if (typeof where === 'function' && typeof callback !== 'function') {
                callback = where;
                where = undefined;
            }

            if (typeof table === 'function' && typeof callback !== 'function') {
                callback = table;
                table = undefined;
                where = undefined;
            }

            // if (typeof callback !== 'function') {
            //     throw new Error("delete(): No callback function has been provided!");
            // }

            const sql = qb.delete(table, where);

            qb.reset_query(sql);
            if (typeof callback !== "function") return new WrapperPromise(sql, exec).promisify();
            exec(sql, callback);
        },

        empty_table: function (table, callback) {
            const sql = qb.empty_table(table, callback);
            qb.reset_query(sql);
            if (typeof callback !== "function") return new WrapperPromise(sql, exec).promisify();
            exec(sql, callback);
        },

        truncate: function (table, callback) {
            const sql = qb.truncate(table, callback);
            qb.reset_query(sql);
            if (typeof callback !== "function") return new WrapperPromise(sql, exec).promisify();
            exec(sql, callback);
        },
    }
}



exports.QueryExec = QueryExec;
