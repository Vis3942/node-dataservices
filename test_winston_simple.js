const path = require('path');
const { MESSAGE } = require('triple-beam');
const winston = require('winston');

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
        winston.format.timestamp({format: 'MMM-DD-YYYY HH:mm:ss'}),
        winston.format.align(),
        winston.format.printf(info => `${info.level}: ${[info.timestamp]}: ${info.message}`),
  ),
  transports: [
    new winston.transports.File({
      filename: path.join(__dirname, 'mylogs.txt'),
      level: 'info',
      maxsize: 500
    })
  ]
});


//  format: winston.format.printf(info => `${info.message}`),

// Write 750 characters
logger.info(`test=${'a'.repeat(245)}`);
logger.info(`test=${'b'.repeat(245)}`);
logger.info(`test=${'c'.repeat(245)}`);

setTimeout(() => {
  logger.info(`test=${'d'.repeat(245)}`);
  logger.info(`test=${'e'.repeat(245)}`);
  logger.info(`test=${'f'.repeat(245)}`);
}, 2000);