"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SchemaExtractionTool = void 0;
const winston = require("winston");
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.json(),
    defaultMeta: { app: 'mongodb-schema-extraction-tool' },
    transports: [new winston.transports.Console()],
});
class SchemaExtractionTool {
    static async connect(mongoClient, databaseName, options) {
        var _a, _b;
        const instanceOptions = Object.assign(SchemaExtractionTool.defaultOptions, options);
        try {
            await mongoClient.connect();
            (_a = instanceOptions.logger) === null || _a === void 0 ? void 0 : _a.info('connected.');
            const database = mongoClient.db(databaseName);
            const instance = new SchemaExtractionTool(database, instanceOptions);
            return instance;
        }
        catch (error) {
            (_b = instanceOptions.logger) === null || _b === void 0 ? void 0 : _b.info('connected.');
            throw error;
        }
    }
    constructor(database, options) {
        this.database = database;
        this.options = options;
    }
    getFieldType(value) {
        if (Array.isArray(value)) {
            return 'array';
        }
        return typeof value;
    }
    async getSubFields(value, fieldType, depth) {
        if (fieldType === 'array' && value && value.length > 0) {
            const firstElement = value[0];
            if (Array.isArray(firstElement) || typeof firstElement === 'object') {
                return this.getSchemaFromArrayDocs(value, depth + 1);
            }
            else {
                return undefined;
            }
        }
        else if (fieldType === 'object') {
            return this.getSchemaFromArrayDocs([value], depth + 1);
        }
        return undefined;
    }
    async getSchemaFromArrayDocs(arrayDocs, depth) {
        const schema = Array();
        for (const doc of arrayDocs) {
            for (const key in doc) {
                const value = doc[key];
                const fieldType = this.getFieldType(value);
                const subFields = await this.getSubFields(value, fieldType, depth);
                schema.push({ name: key, types: [fieldType], subFields });
            }
        }
        return schema;
    }
    async getSchemaFromSubDocs(collectionName, field, depth) {
        const subDocs = await this.database
            .collection(collectionName)
            .aggregate([{ $project: { subDoc: `$${field}` } }, { $match: { subDoc: { $type: 'object' } } }])
            .toArray();
        const schema = Array();
        for (const subDoc of subDocs) {
            const subSchema = await this.getSchemaFromArrayDocs([subDoc.subDoc], depth);
            schema.push(...subSchema);
        }
        return schema;
    }
    async getSchema(collectionName, depth = 0) {
        const pipeline = [
            { $project: { flat: { $objectToArray: '$$ROOT' } } },
            { $unwind: '$flat' },
            { $group: { _id: '$flat.k', types: { $addToSet: { $type: '$flat.v' } } } },
        ];
        const result = await this.database.collection(collectionName).aggregate(pipeline).toArray();
        const schema = Array();
        for (const field of result) {
            const fieldType = field.types.includes('object') ? 'object' : field.types[0];
            let subFields = undefined;
            if (fieldType === 'object') {
                subFields = await this.getSchemaFromSubDocs(collectionName, field._id, depth + 1);
            }
            schema.push({ name: field._id, types: [fieldType], subFields });
        }
        return schema;
    }
    async generateSchemaForAllCollections() {
        var _a, _b;
        const collections = await this.database.listCollections().toArray();
        const schema = {};
        (_a = this.options.logger) === null || _a === void 0 ? void 0 : _a.info(`generating schema for ${collections.length} collections`);
        for (const collection of collections) {
            const collectionName = collection.name;
            schema[collectionName] = await this.getSchema(collectionName);
            (_b = this.options.logger) === null || _b === void 0 ? void 0 : _b.info(`"${collectionName}" generated.`);
        }
        return schema;
    }
    flattenSchema(schema) {
        const flattenedSchema = {};
        const flattenFields = (fields, parentField = '') => {
            const result = {};
            fields.forEach(field => {
                const fieldName = parentField ? `${parentField}.${field.name}` : field.name;
                if (!this.options.filteredFields.some(filteredField => fieldName.includes(filteredField))) {
                    result[fieldName] = field.types.join();
                    if (field.subFields) {
                        const subFields = flattenFields(field.subFields, fieldName);
                        Object.assign(result, subFields);
                    }
                }
            });
            return result;
        };
        for (const collectionName in schema) {
            flattenedSchema[collectionName] = flattenFields(schema[collectionName]);
        }
        return flattenedSchema;
    }
}
exports.SchemaExtractionTool = SchemaExtractionTool;
SchemaExtractionTool.defaultOptions = {
    filteredFields: ['.buffer', '__v'],
    logger: logger,
};
//# sourceMappingURL=schema-extraction-tool.js.map