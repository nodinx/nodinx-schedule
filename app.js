'use strict';

const qs = require('querystring');
const path = require('path');
const parser = require('cron-parser');
const ms = require('humanize-ms');
const loadSchedule = require('./lib/load_schedule');

const SCHEDULE_HANDLER = Symbol.for('egg#scheduleHandler');

module.exports = app => {
  const schedules = loadSchedule(app);
  listenScheduleMessage();
  // add handler into `agent[SCHEDULE_HANDLER]` for extend other kind of schedule type.
  // worker: will excute in one random worker when schedule excuted.
  // all: will excute in all workers when schedule excuted.
  const handlers = app[SCHEDULE_HANDLER] = {
    worker: workerHandler,
    all: allHander,
  };

  app.messenger.once('egg-ready', startSchedule);

  function startSchedule() {
    app.coreLogger.info("[nodinx-schedule] start schedule ...");
    app.disableSchedule = false;
    for (const s in schedules) {
      const schedule = schedules[s];
      if (schedule.schedule.disable) continue;

      const type = schedule.schedule.type;
      const handler = handlers[type];
      if (!handler) {
        const err = new Error(`schedule type [${type}] is not defined`);
        err.name = 'EggScheduleError';
        throw err;
      }
      handler(schedule.schedule, {
        one() {
          sendMessage(app, 'send', schedule.key); // sendRandom -> send
        },
        all() {
          sendMessage(app, 'send', schedule.key);
        },
      });
    }
  }

  app.on('close', () => {
    app.disableSchedule = true;
    return;
  });

  // for test purpose
  app.runSchedule = schedulePath => {
    if (!path.isAbsolute(schedulePath)) {
      schedulePath = path.join(app.config.baseDir, 'app/schedule', schedulePath);
    }
    schedulePath = require.resolve(schedulePath);
    let schedule;

    try {
      schedule = schedules[schedulePath];
      if (!schedule) {
        throw new Error(`Cannot find schedule ${schedulePath}`);
      }
    } catch (err) {
      err.message = `[nodinx-schedule] ${err.message}`;
      return Promise.reject(err);
    }

    // run with anonymous context
    const ctx = app.createAnonymousContext({
      method: 'SCHEDULE',
      url: `/__schedule?path=${schedulePath}&${qs.stringify(schedule.schedule)}`,
    });
    return schedule.task(ctx);
  };

  /**
   * 监听调度任务的消息
   */
  function listenScheduleMessage() {
    for (const s in schedules) {
      const schedule = schedules[s];
      if (schedule.schedule.disable) continue;

      const task = schedule.task;
      const key = schedule.key;
      app.coreLogger.info('[nodinx-schedule]: register schedule %s', key);
      app.messenger.on(key, () => {
        app.coreLogger.info('[nodinx-schedule]: get message %s', key);

      // run with anonymous context
        const ctx = app.createAnonymousContext({
          method: 'SCHEDULE',
          url: `/__schedule?path=${s}&${qs.stringify(schedule.schedule)}`,
        });

        const start = Date.now();
        task(ctx)
      .then(() => true) // succeed
      .catch(err => {
        err.message = `[nodinx-schedule] ${key} excute error. ${err.message}`;
        app.logger.error(err);
        return false;   // failed
      })
      .then(success => {
        const rt = Date.now() - start;
        const status = success ? 'succeed' : 'failed';
        app.coreLogger.info(`[nodinx-schedule] ${key} excute ${status}, used ${rt}ms`);
      });
      });
    }
  }
};

function sendMessage(app, method, key) {
  if (app.disableSchedule) {
    app.coreLogger.info(`[nodinx-schedule] message ${key} did not sent`);
    return;
  }
  app.coreLogger.info(`[nodinx-schedule] send message: ${method} ${key}`);
  app.messenger[method](key);
}

function workerHandler(schedule, sender) {
  baseHander(schedule, sender.one);
}

function allHander(schedule, sender) {
  baseHander(schedule, sender.all);
}

function baseHander(schedule, send) {
  if (!schedule.interval && !schedule.cron) {
    throw new Error('[nodinx-schedule] schedule.interval or schedule.cron must be present');
  }

  if (schedule.interval) {
    const interval = ms(schedule.interval);
    setInterval(send, interval);
  }

  if (schedule.cron) {
    let interval;
    try {
      interval = parser.parseExpression(schedule.cron);
    } catch (err) {
      err.message = `[nodinx-schedule] parse cron instruction(${schedule.cron}) error: ${err.message}`;
      throw err;
    }
    startCron(interval, send);
  }

  if (schedule.immediate) {
    setImmediate(send);
  }
}

function startCron(interval, listener) {
  const now = Date.now();
  let nextTick;
  do {
    nextTick = interval.next().getTime();
  } while (now >= nextTick);

  setTimeout(() => {
    listener();
    startCron(interval, listener);
  }, nextTick - now);
}