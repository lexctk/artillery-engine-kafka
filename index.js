const kafka = require('kafka-node')
const debug = require('debug')('engine:kafka')
const A = require('async')

const { log: logger } = console

const replaceDoubleBraces = (str,result) =>{
	return str.replace(/{{(.+?)}}/g, (_,g1) => result[g1] || g1)
}

const createProducer = (client, scenarioSpec) => {
	const producer = scenarioSpec.some(scenario =>
		scenario.flow.some(
			({ publishMessage = {} }) =>
				publishMessage && 'partition' in publishMessage,
		),
	)

	return producer
		? new kafka.Producer(client)
		: new kafka.HighLevelProducer(client)
}

const getPayload = length =>
	Buffer.alloc(
		length,
		Math.random()
			.toString(36)
			.replace(/[^a-z]+/g, ''),
	)

const createMessage = data =>
	typeof data === 'object' ? JSON.stringify(data) : String(data)

class KafkaEngine {
	constructor(script, ee, helpers) {
		this.script = script;
		this.ee = ee;
		this.helpers = helpers;

		return this;
	}

	createScenario(scenarioSpec, ee) {
		const { beforeScenario = [], afterScenario = [] } = scenarioSpec;

		const beforeScenarioFns = beforeScenario.map((hookFunctionName) => {
			return { function: hookFunctionName };
		});

		const afterScenarioFns = afterScenario.map((hookFunctionName) => {
			return { function: hookFunctionName };
		});

		scenarioSpec.flow = beforeScenarioFns.concat(
			scenarioSpec.flow.concat(afterScenarioFns)
		);

		const tasks = scenarioSpec.flow.map((rs) => this.step(rs, ee));

		return this.compile(tasks, scenarioSpec.flow, ee);
	}

	step(rs, ee) {
		const self = this

		if (rs.loop) {
			const steps = rs.loop.map(loopStep => this.step(loopStep, ee))

			return this.helpers.createLoopWithCount(rs.count || -1, steps, {})
		}

		if (rs.log) {
			return function log(context, callback) {
				logger(self.helpers.template(rs.log, context))

				return process.nextTick(() => {
					callback(null, context)
				})
			}
		}

		if (rs.think) {
			return this.helpers.createThink(
				rs,
				self.script.config.defaults?.think || {}
			);
		}

		if (rs.function) {
			return function (context, callback) {
				let processFunc = self.script.config.processor[rs.function];
				if (processFunc) {
					if (processFunc.constructor.name === 'Function') {
						return processFunc(context, ee, function (hookErr) {
							return callback(hookErr, context);
						});
					} else {
						return processFunc(context, ee)
							.then(() => {
								callback(null, context);
							})
							.catch((err) => {
								callback(err, context);
							});
					}
				} else {
					debug(`Function "${rs.function}" not defined`);
					debug('processor: %o', self.script.config.processor);
					ee.emit('error', `Undefined function "${rs.function}"`);
					return process.nextTick(function () {
						callback(null, context);
					});
				}
			};
		}

		if (rs.publishMessage) {
			return function publishMessage(context, callback) {
				const batchSize = Number(rs.publishMessage.batch) || 1
				const { producer } = context.kafka;
				const replaceVariables = JSON.parse(replaceDoubleBraces(JSON.stringify(rs.publishMessage), context.vars));
				const { topic, data, size, partition } = replaceVariables;

				const message = {
					topic,
					messages: new Array(batchSize)
						.fill()
						.map(() =>
							data
								? createMessage(data)
								: getPayload(Number(size) || DEFAULT_MESSAGE_SIZE),
						),
				}

				if (partition) {
					message.partition = partition
				}

				producer.send([message], err => {
					if (err) {
						ee.emit('error', err)
						debug(err)
						logger(err);

						return callback(err, context)
					}

					ee.emit('response', 0, 0, context._uid)

					return callback(null, context)
				})
			}
		}

		return function (context, callback) {
			return callback(null, context);
		};
	}

	compile(tasks, scenarioSpec, ee) {
		const self = this

		return function scenario(initialContext, callback) {
			const init = function init(next) {
				if (!((self.script.config.kafka || {}).client || {}).kafkaHost) {
					throw new Error('kafka.client.kafkaHost is required')
				}

				const {
					kafka: { client: opts },
				} = self.script.config

				const kafkaClient = new kafka.KafkaClient(opts)
				const producer = createProducer(kafkaClient, self.script.scenarios)

				producer.on('error', err => {
					ee.emit('error', err)
				})

				producer.on('ready', () => {
					ee.emit('started')

					next(
						null,
						Object.assign(initialContext, {
							kafka: {
								producer,
							},
						}),
					)
				})
			}

			const steps = [init].concat(tasks)

			A.waterfall(steps, (err, context) => {
				if (err) {
					debug(err)
				}

				return callback(err, context)
			})
		}
	}
}


module.exports = KafkaEngine;
