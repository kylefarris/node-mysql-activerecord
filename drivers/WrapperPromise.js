/**
 * 
 * @param {String} sql - A sql command 
 * @param {function} exec - A sql execution function
 * @param {function} callback - A callback function
 * 
 * @description
 * Wraps the execute command in promise
 */
var WrapperPromise = function (sql, exec, callback) {
    this.sql = sql;
    this.exec = exec;
    if (typeof callback === "function")
        this.callback = callback.bind(this);

};

/**
 * Promisify 
 */
WrapperPromise.prototype.promisify = function () {
    return new Promise((resolve, reject) => {
        this.resolve = resolve;
        this.reject = reject;

        this.invoke()
    })
}

/**
 * Executet the query and resolve as the result
 */
WrapperPromise.prototype.invoke = function () {
    this.exec(this.sql, (err, res) => {
        if (err) {
            this.reject(err);
            return;
        }

        /**
         * If theres is a callback function. let the callback function resolve and reject the promise
         */
        if (typeof this.callback === "function") { this.callback(err,res); return; }

        this.resolve(res);
    })
}

module.exports = WrapperPromise;