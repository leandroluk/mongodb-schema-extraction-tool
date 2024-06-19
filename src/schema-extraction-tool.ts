import {Db, Document, MongoClient} from 'mongodb';
import * as winston from 'winston';

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  defaultMeta: {app: 'mongodb-schema-extraction-tool'},
  transports: [new winston.transports.Console()],
});

export namespace SchemaExtractionTool {
  export type FieldSchema = {
    /**
     * Refer to name of field in schema
     */
    name: string;
    /**
     * All available types extracted from mongo aggregation {$type: '$flat.v'}
     */
    types: string[];
    /**
     * All available subfields over this field
     */
    subFields?: FieldSchema[];
  };

  /**
   * The extracted schema for a collection
   */
  export type FullSchema = {
    [collectionName: string]: FieldSchema[];
  };

  /** The simplified and flatten schema created from FullSchema */
  export type FlattenSchema = {
    [collectionName: string]: {
      [fieldName: string]: string;
    };
  };

  export type Logger = {
    info: typeof console.info;
    error: typeof console.error;
  };

  /**
   * Available class options
   */
  export type Options = {
    /**
     * A list of included patterns to exclude when extract schema.
     * @default ['.buffer', '__v']
     */
    filteredFields?: string[];
    /**
     * Custom logger for extraction process. By default use Winston logger in json format
     */
    logger?: Logger;
  };
}

export class SchemaExtractionTool {
  static readonly defaultOptions: SchemaExtractionTool.Options = {
    filteredFields: ['.buffer', '__v'],
    logger: logger,
  };

  static async connect(
    mongoClient: MongoClient,
    databaseName: string,
    options?: SchemaExtractionTool.Options
  ): Promise<SchemaExtractionTool> {
    const instanceOptions = Object.assign(SchemaExtractionTool.defaultOptions, options);
    try {
      await mongoClient.connect();
      instanceOptions.logger?.info('connected.');
      const database = mongoClient.db(databaseName);
      const instance = new SchemaExtractionTool(database, instanceOptions);
      return instance;
    } catch (error) {
      instanceOptions.logger?.info('connected.');
      throw error;
    }
  }

  private constructor(
    private readonly database: Db,
    private readonly options: SchemaExtractionTool.Options
  ) {}

  private getFieldType(value: Document): string {
    if (Array.isArray(value)) {
      return 'array';
    }
    return typeof value;
  }

  private async getSubFields(
    value: Document,
    fieldType: string,
    depth: number
  ): Promise<SchemaExtractionTool.FieldSchema[] | undefined> {
    if (fieldType === 'array' && value && (value as Array<never>).length > 0) {
      const firstElement = (value as Array<never>)[0];
      if (Array.isArray(firstElement) || typeof firstElement === 'object') {
        return this.getSchemaFromArrayDocs(value as Array<never>, depth + 1);
      } else {
        return undefined;
      }
    } else if (fieldType === 'object') {
      return this.getSchemaFromArrayDocs([value], depth + 1);
    }
    return undefined;
  }

  private async getSchemaFromArrayDocs(
    arrayDocs: Document[],
    depth: number
  ): Promise<SchemaExtractionTool.FieldSchema[]> {
    const schema = Array<SchemaExtractionTool.FieldSchema>();
    for (const doc of arrayDocs) {
      for (const key in doc) {
        const value = doc[key];
        const fieldType = this.getFieldType(value);
        const subFields = await this.getSubFields(value, fieldType, depth);
        schema.push({name: key, types: [fieldType], subFields});
      }
    }
    return schema;
  }

  private async getSchemaFromSubDocs(
    collectionName: string,
    field: string,
    depth: number
  ): Promise<SchemaExtractionTool.FieldSchema[]> {
    const subDocs = await this.database
      .collection(collectionName)
      .aggregate([{$project: {subDoc: `$${field}`}}, {$match: {subDoc: {$type: 'object'}}}])
      .toArray();
    const schema = Array<SchemaExtractionTool.FieldSchema>();
    for (const subDoc of subDocs) {
      const subSchema = await this.getSchemaFromArrayDocs([subDoc.subDoc], depth);
      schema.push(...subSchema);
    }
    return schema;
  }

  private async getSchema(collectionName: string, depth = 0): Promise<SchemaExtractionTool.FieldSchema[]> {
    const pipeline = [
      {$project: {flat: {$objectToArray: '$$ROOT'}}},
      {$unwind: '$flat'},
      {$group: {_id: '$flat.k', types: {$addToSet: {$type: '$flat.v'}}}},
    ];
    const result = await this.database.collection(collectionName).aggregate(pipeline).toArray();
    const schema = Array<SchemaExtractionTool.FieldSchema>();
    for (const field of result) {
      const fieldType = field.types.includes('object') ? 'object' : field.types[0];
      let subFields = undefined;
      if (fieldType === 'object') {
        subFields = await this.getSchemaFromSubDocs(collectionName, field._id, depth + 1);
      }
      schema.push({name: field._id, types: [fieldType], subFields});
    }
    return schema;
  }

  async generateSchemaForAllCollections(): Promise<SchemaExtractionTool.FullSchema> {
    const collections = await this.database.listCollections().toArray();
    const schema: {[key: string]: SchemaExtractionTool.FieldSchema[]} = {};
    this.options.logger?.info(`generating schema for ${collections.length} collections`);
    for (const collection of collections) {
      const collectionName = collection.name;
      schema[collectionName] = await this.getSchema(collectionName);
      this.options.logger?.info(`collection "${collectionName}" generated.`);
    }
    return schema;
  }

  flattenSchema(schema: SchemaExtractionTool.FullSchema): SchemaExtractionTool.FlattenSchema {
    const flattenedSchema: SchemaExtractionTool.FlattenSchema = {};
    const flattenFields = (fields: SchemaExtractionTool.FieldSchema[], parentField = ''): {[key: string]: string} => {
      const result: {[key: string]: string} = {};
      fields.forEach(field => {
        const fieldName = parentField ? `${parentField}.${field.name}` : field.name;
        if (!this.options.filteredFields!.some(filteredField => fieldName.includes(filteredField))) {
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
