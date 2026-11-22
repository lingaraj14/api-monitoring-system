import amqp from "amqplib"
import config from "./index.js"
import logger from "./logger.js"

class RabbitMQConnection {
    constructor() {
        this.connection = null;
        this.channel = null;
        this.isConnected = false;
    }

    async connect() {
        if(this.channel) {
            return this.channel;
        }

        if(this.isConnected) {
            await new Promise((resolve) => {
                const checkInterval = setInterval(() => {
                    if(this.isConnected) {
                        clearInterval(checkInterval);
                        resolve();
                    }
                }, 100);
            });

            return this.channel;
        }

        try {
            this.isConnected = true;

            logger.info(`Connecting to RabbitMQ at ${config.rabbitmq.url}`);
            this.connection = await amqp.connect(config.rabbitmq.url);
            this.channel = await this.connection.createChannel();

            //Creating Key | Queue name
            const dlqName = `${config.rabbitmq.queue}.dlq`;  //api_hits | api_hits.dlq

            //DL Queue
            await this.channel.assertQueue(dlqName, { durable: true });

            //Normal Queue
            await this.channel.assertQueue(config.rabbitmq.queue, {
                durable: true,
                arguments: {
                    "x-dead-letter-exchange": "", // Default exchange
                    "x-dead-letter-routing-key": dlqName, // Route to DLQ
                },
            });

            logger.info(`RabbitMQ connected queue: ${config.rabbitmq.queue}`);
            
            this.connection.on("close", () => {
                logger.warn("RabbitMQ connection closed");
                this.connection = null;
                this.channel = null;
            });

            this.connection.on("error", (err) => {
                logger.error("RabbitMQ connection error:", err);
                this.connection = null;
                this.channel = null;
            });

            this.isConnected = false;
            return this.channel;

        }catch (error) {
            this.isConnected = false;
            logger.error("Failed to connect to RabbitMQ:", error);
            throw error;
        }
    }

    getChannel() {
        if(!this.channel) {
            throw new Error("RabbitMQ channel is not established. Call connect() first.");
        }
        return this.channel;
    }

    async close() {
        try {
            if(this.channel) {
                await this.channel.close();
                this.channel = null;
            }
            if(this.connection) {
                await this.connection.close();
                this.connection = null;
            }
            logger.info("RabbitMQ connection closed");
        } catch (error) {
            logger.error("Failed to close RabbitMQ connection:", error);
        }
    }

}

export default new RabbitMQConnection();

//Note: Need to complete the RabbitMQ videosß