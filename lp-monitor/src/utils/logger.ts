// src/utils/logger.ts
import winston from 'winston';
import { TransformableInfo } from 'logform';
import path from 'path';
import fs from 'fs';

// Get log directory from environment or use default with absolute path
const logDir = process.env.LP_HEDGE_LOG_DIR || path.join(process.cwd(), '../logs');

// Create log directory if it doesn't exist (using sync for startup)
try {
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }
} catch (error) {
  console.error('Error creating log directory:', error);
}

// Configure Winston logger with absolute paths
export const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp({
      format: 'YYYY-MM-DD HH:mm:ss,SSS'
    }),
    winston.format.printf((info: TransformableInfo) => {
      const message = typeof info.message === 'string' ? info.message : String(info.message);
      return `${info.timestamp} - main - ${info.level.toUpperCase()} - ${message}`;
    })
  ),
  transports: [
    new winston.transports.File({
      filename: path.join(logDir, 'lp-monitor.log'),
      maxsize: 5242880, // 5MB
      maxFiles: 5,
    }),
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.timestamp({
          format: 'YYYY-MM-DD HH:mm:ss,SSS'
        }),
        winston.format.printf((info: TransformableInfo) => {
          const message = typeof info.message === 'string' ? info.message : String(info.message);
          return `${info.timestamp} - main - ${info.level.toUpperCase()} - ${message}`;
        })
      )
    })
  ]
});