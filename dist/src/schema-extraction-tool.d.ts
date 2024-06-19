import { MongoClient } from 'mongodb';
export declare namespace SchemaExtractionTool {
    type FieldSchema = {
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
    type FullSchema = {
        [collectionName: string]: FieldSchema[];
    };
    /** The simplified and flatten schema created from FullSchema */
    type FlattenSchema = {
        [collectionName: string]: {
            [fieldName: string]: string;
        };
    };
    type Logger = {
        info: typeof console.info;
        error: typeof console.error;
    };
    /**
     * Available class options
     */
    type Options = {
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
export declare class SchemaExtractionTool {
    private readonly database;
    private readonly options;
    static readonly defaultOptions: SchemaExtractionTool.Options;
    static connect(mongoClient: MongoClient, databaseName: string, options?: SchemaExtractionTool.Options): Promise<SchemaExtractionTool>;
    private constructor();
    private getFieldType;
    private getSubFields;
    private getSchemaFromArrayDocs;
    private getSchemaFromSubDocs;
    private getSchema;
    generateSchemaForAllCollections(): Promise<SchemaExtractionTool.FullSchema>;
    flattenSchema(schema: SchemaExtractionTool.FullSchema): SchemaExtractionTool.FlattenSchema;
}
