import * as fs from 'fs';
import {Db, Document, MongoClient} from 'mongodb';
import * as path from 'path';
import {SchemaExtractionTool} from './schema-extraction-tool';

interface FieldSchema {
  field: string;
  types: string[];
  subFields?: FieldSchema[];
}

type Schema = {
  [collectionName: string]: {
    [fieldName: string]: string[];
  };
};

async function getSchema(collectionName: string, db: Db, depth = 0): Promise<FieldSchema[]> {
  const pipeline = [
    {$project: {flat: {$objectToArray: '$$ROOT'}}},
    {$unwind: '$flat'},
    {$group: {_id: '$flat.k', types: {$addToSet: {$type: '$flat.v'}}}},
  ];
  const result = await db.collection(collectionName).aggregate(pipeline).toArray();
  const schema = Array<FieldSchema>();
  for (const field of result) {
    const fieldType = field.types.includes('object') ? 'object' : field.types[0];
    let subFields = undefined;
    if (fieldType === 'object') {
      subFields = await getSchemaFromSubDocs(db, collectionName, field._id, depth + 1);
    }
    schema.push({field: field._id, types: [fieldType], subFields});
  }
  return schema;
}

async function getSchemaFromSubDocs(
  db: Db,
  collectionName: string,
  field: string,
  depth: number
): Promise<FieldSchema[]> {
  const subDocs = await db
    .collection(collectionName)
    .aggregate([{$project: {subDoc: `$${field}`}}, {$match: {subDoc: {$type: 'object'}}}])
    .toArray();

  const schema: FieldSchema[] = [];

  for (const subDoc of subDocs) {
    const subSchema = await getSchemaFromArrayDocs([subDoc.subDoc], depth);
    schema.push(...subSchema);
  }

  return schema;
}

async function getSchemaFromArrayDocs(arrayDocs: Document[], depth: number): Promise<FieldSchema[]> {
  const schema: FieldSchema[] = [];
  for (const doc of arrayDocs) {
    for (const key in doc) {
      const value = doc[key];
      const fieldType = getFieldType(value);
      const subFields = await getSubFields(value, fieldType, depth);
      schema.push({field: key, types: [fieldType], subFields});
    }
  }
  return schema;
}

function getFieldType(value: any): string {
  if (Array.isArray(value)) {
    return 'array';
  }
  return typeof value;
}

async function getSubFields(value: any, fieldType: string, depth: number): Promise<FieldSchema[] | undefined> {
  if (fieldType === 'array' && value.length > 0) {
    const firstElement = value[0];
    if (Array.isArray(firstElement) || typeof firstElement === 'object') {
      return getSchemaFromArrayDocs(value, depth + 1);
    } else {
      return undefined;
    }
  } else if (fieldType === 'object') {
    return getSchemaFromArrayDocs([value], depth + 1);
  }
  return undefined;
}

async function generateSchemaForAllCollections(db: Db): Promise<{[key: string]: FieldSchema[]}> {
  const collections = await db.listCollections().toArray();
  const schema: {[key: string]: FieldSchema[]} = {};
  console.log(`generating schema for ${collections.length} collections:`);
  for (const collection of collections) {
    const collectionName = collection.name;
    schema[collectionName] = await getSchema(collectionName, db);
    console.log(`- "${collectionName}" generated`);
  }
  return schema;
}

const filteredFields = ['.buffer', '__v'];

function flattenSchema(schema: {[key: string]: FieldSchema[]}): Schema {
  const flattenedSchema: Schema = {};

  function flattenFields(fields: FieldSchema[], parentField = ''): {[key: string]: string[]} {
    const result: {[key: string]: string[]} = {};

    fields.forEach(item => {
      const fieldName = parentField ? `${parentField}.${item.field}` : item.field;
      if (!filteredFields.some(filter => fieldName.includes(filter))) {
        result[fieldName] = item.types.join() as unknown as string[];
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
  const mongoClient = new MongoClient(process.env.MONGO_URL as string);
  const tool = await SchemaExtractionTool.connect(mongoClient, process.env.MONGO_DATABASE!);

  const schema = await tool.generateSchemaForAllCollections();
  const flattened = tool.flattenSchema(schema);

  const tempDir = path.resolve(process.cwd(), '.tmp');
  const encoding = 'utf-8';

  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir);
  }

  const jsonSchema = JSON.stringify(schema, null, 2);
  const jsonFlattened = JSON.stringify(flattened, null, 2);
  const jsonSchemaPath = path.resolve(tempDir, `${process.env.PREFIX_FILE}-schema.json`);
  const jsonFlattenedPath = path.resolve(tempDir, `${process.env.PREFIX_FILE}-flattened.json`);

  fs.writeFileSync(jsonSchemaPath, jsonSchema, encoding);
  fs.writeFileSync(jsonFlattenedPath, jsonFlattened, encoding);

  await mongoClient.close();
}

main()
  .then(() => process.exit(0))
  .catch(e => [console.error(e), process.exit(1)]);
