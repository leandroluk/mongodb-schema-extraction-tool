"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const fs = require("fs");
const mongodb_1 = require("mongodb");
const path = require("path");
const schema_extraction_tool_1 = require("./schema-extraction-tool");
async function getSchema(collectionName, db, depth = 0) {
    const pipeline = [
        { $project: { flat: { $objectToArray: '$$ROOT' } } },
        { $unwind: '$flat' },
        { $group: { _id: '$flat.k', types: { $addToSet: { $type: '$flat.v' } } } },
    ];
    const result = await db.collection(collectionName).aggregate(pipeline).toArray();
    const schema = Array();
    for (const field of result) {
        const fieldType = field.types.includes('object') ? 'object' : field.types[0];
        let subFields = undefined;
        if (fieldType === 'object') {
            subFields = await getSchemaFromSubDocs(db, collectionName, field._id, depth + 1);
        }
        schema.push({ field: field._id, types: [fieldType], subFields });
    }
    return schema;
}
async function getSchemaFromSubDocs(db, collectionName, field, depth) {
    const subDocs = await db
        .collection(collectionName)
        .aggregate([{ $project: { subDoc: `$${field}` } }, { $match: { subDoc: { $type: 'object' } } }])
        .toArray();
    const schema = [];
    for (const subDoc of subDocs) {
        const subSchema = await getSchemaFromArrayDocs([subDoc.subDoc], depth);
        schema.push(...subSchema);
    }
    return schema;
}
async function getSchemaFromArrayDocs(arrayDocs, depth) {
    const schema = [];
    for (const doc of arrayDocs) {
        for (const key in doc) {
            const value = doc[key];
            const fieldType = getFieldType(value);
            const subFields = await getSubFields(value, fieldType, depth);
            schema.push({ field: key, types: [fieldType], subFields });
        }
    }
    return schema;
}
function getFieldType(value) {
    if (Array.isArray(value)) {
        return 'array';
    }
    return typeof value;
}
async function getSubFields(value, fieldType, depth) {
    if (fieldType === 'array' && value.length > 0) {
        const firstElement = value[0];
        if (Array.isArray(firstElement) || typeof firstElement === 'object') {
            return getSchemaFromArrayDocs(value, depth + 1);
        }
        else {
            return undefined;
        }
    }
    else if (fieldType === 'object') {
        return getSchemaFromArrayDocs([value], depth + 1);
    }
    return undefined;
}
async function generateSchemaForAllCollections(db) {
    const collections = await db.listCollections().toArray();
    const schema = {};
    console.log(`generating schema for ${collections.length} collections:`);
    for (const collection of collections) {
        const collectionName = collection.name;
        schema[collectionName] = await getSchema(collectionName, db);
        console.log(`- "${collectionName}" generated`);
    }
    return schema;
}
const filteredFields = ['.buffer', '__v'];
function flattenSchema(schema) {
    const flattenedSchema = {};
    function flattenFields(fields, parentField = '') {
        const result = {};
        fields.forEach(item => {
            const fieldName = parentField ? `${parentField}.${item.field}` : item.field;
            if (!filteredFields.some(filter => fieldName.includes(filter))) {
                result[fieldName] = item.types.join();
                if (item.subFields) {
                    const subFields = flattenFields(item.subFields, fieldName);
                    Object.assign(result, subFields);
                }
            }
        });
        return result;
    }
    for (const collectionName in schema) {
        flattenedSchema[collectionName] = flattenFields(schema[collectionName]);
    }
    return flattenedSchema;
}
async function main() {
    const mongoClient = new mongodb_1.MongoClient(process.env.MONGO_URL);
    const tool = await schema_extraction_tool_1.SchemaExtractionTool.connect(mongoClient, process.env.MONGO_DATABASE);
    const schema = await tool.generateSchemaForAllCollections();
    const flattened = tool.flattenSchema(schema);
    const jsonSchema = JSON.stringify(schema, null, 2);
    const jsonFlattened = JSON.stringify(flattened, null, 2);
    const jsonSchemaPath = path.resolve(process.cwd(), `${process.env.PREFIX_FILE}-schema.json`);
    const jsonFlattenedPath = path.resolve(process.cwd(), `${process.env.PREFIX_FILE}-flattened.json`);
    const encoding = 'utf-8';
    fs.writeFileSync(jsonSchemaPath, jsonSchema, encoding);
    fs.writeFileSync(jsonFlattenedPath, jsonFlattened, encoding);
    await mongoClient.close();
}
main()
    .then(() => {
    console.log('finished');
    process.exit(0);
})
    .catch(e => {
    console.error(e);
    process.exit(1);
});
//# sourceMappingURL=runner.js.map