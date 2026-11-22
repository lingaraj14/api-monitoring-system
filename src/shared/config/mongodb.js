/**
 * Date: 16-11-2026 05:17PM
 * This mongoDB connection code follows the Singleton design pattern to ensure that only one instance of the connection is created and reused throughout the application.
 */
import mongoose from "mongoose";
import config from "./index.js";
import logger from "./logger.js";

/**
 * MongoDB database manager/connector
 */
class MongoDBConnection {
    constructor() {
        this.connection = null;
    }

    /**
     * Connect to MongoDB database using Mongoose
     * @returns {Promise<mongoose.Connection>} - Returns a promise that resolves to the Mongoose connection object
     * 
     */
    async connect() {
        try {
            if(this.connection) {
                logger.info("MongoDB already connected");
                return this.connection;
            }

            await mongoose.connect(config.mongo.uri, {
                dbName: config.mongo.dbName
            });

            this.connection = mongoose.connection;

            logger.info(`MongoDB connected to ${config.mongo.uri} with dbName: ${config.mongo.dbName}`);

            this.connection.on("error", (err) => {
                logger.error("MongoDB connection error:", err);
            });

            this.connection.on("disconnected", () => {
                logger.warn("MongoDB disconnected");
            });

            return this.connection;

        } catch (error) {
            logger.error("Failed to connect to MongoDB:", error);
            throw error;
        }
    }

    /**
     * This method disconnects from the MongoDB database if a connection exists.
     * If there is no active connection, it logs a warning message.
     */
    async disconnect() {
        try {
            if(this.connection) {
                await mongoose.disconnect();
                this.connection = null;
                logger.info("MongoDB disconnected");
            } else {
                logger.warn("MongoDB is not connected");
            }
        } catch (error) {
            logger.error("Failed to disconnect from MongoDB:", error);
            throw error;
        }
    }

    /**
     * Gate the active Mongoose connection. If the connection is not established, it throws an error.
     * @returns {mongoose.Connection} - Returns the active Mongoose connection object
     */
    getConnection() {
        if(!this.connection) {
            throw new Error("MongoDB is not connected. Call connect() first.");
        }
        return this.connection;
    }
}

export default new MongoDBConnection();
