var log4js = require('log4js')
  , Logger;

exports.init = function (logger) {
  if (logger) {
    Logger = logger;
  } else {
    Logger = log4js.getLogger('[jackrabbit-2]');
  }
};

exports.getLogger = function() {
  return Logger;
};