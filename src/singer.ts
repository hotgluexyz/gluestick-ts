import fs from 'fs-extra';
import * as path from 'path';
import pl from 'nodejs-polars';
import { Reader, Catalog } from './reader.js';

export interface SingerSchema {
  type: string[] | string;
  properties?: Record<string, SingerSchema>;
  items?: SingerSchema;
  format?: string;
}

export interface SingerHeaderMap {
  type: string[];
  properties: Record<string, SingerSchema>;
}

export interface TypeMapping {
  [key: string]: SingerSchema;
}

const TYPE_MAPPING: TypeMapping = {
  float: { type: ["number", "null"] },
  int: { type: ["integer", "null"] },
  bool: { type: ["boolean", "null"] },
  str: { type: ["string", "null"] },
  date: {
    format: "date-time",
    type: ["string", "null"],
  },
  array: { type: ["array", "null"], items: { type: ["object", "string", "null"] } },
};

export function genSingerHeader(
  df: pl.DataFrame,
  allowObjects: boolean,
  schema?: SingerHeaderMap,
  catalogSchema: boolean = false,
  recursiveTyping: boolean = true
): [pl.DataFrame, SingerHeaderMap] {
  let headerMap: SingerHeaderMap = { type: ["object", "null"], properties: {} };
  let modifiedDf = df.clone();

  if (schema && !catalogSchema) {
    headerMap = schema;
    return [df, headerMap];
  }

  for (const col of df.columns) {
    const dtype = df.getColumn(col).dtype.toString().toLowerCase();
    
    // Handle datetime columns
    if (dtype.includes('date') || dtype.includes('time')) {
      // Convert datetime columns to ISO string format
      try {
        modifiedDf = modifiedDf.withColumn(
          pl.col(col).date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        );
      } catch (error) {
        // If conversion fails, keep as string
        console.warn(`Failed to convert datetime column ${col}:`, error);
      }
    }

    const colType = getColumnType(dtype);
    
    if (colType && TYPE_MAPPING[colType]) {
      headerMap.properties[col] = TYPE_MAPPING[colType];
    } else if (allowObjects) {
      const values = df.getColumn(col).toArray().filter(v => v != null);
      
      if (values.length === 0) {
        headerMap.properties[col] = TYPE_MAPPING.str;
        continue;
      }

      const firstValue = values[0];

      if (Array.isArray(firstValue)) {
        if (recursiveTyping) {
          const newInput: any = {};
          for (const row of values) {
            if (Array.isArray(row) && row.length > 0) {
              for (const arrValue of row) {
                if (typeof arrValue === 'object' && arrValue !== null && !Array.isArray(arrValue)) {
                  const tempDict = { ...arrValue };
                  Object.assign(newInput, tempDict);
                } else {
                  Object.assign(newInput, arrValue);
                }
              }
            }
          }
          
          const itemsSchema = toSingerSchema(newInput);
          headerMap.properties[col] = {
            type: ["array", "null"],
            items: itemsSchema
          };
          
          if (Object.keys(newInput).length === 0) {
            headerMap.properties[col] = {
              items: TYPE_MAPPING.str,
              type: ["array", "null"],
            };
          }
        } else {
          headerMap.properties[col] = TYPE_MAPPING.array;
        }
      } else if (typeof firstValue === 'object' && firstValue !== null && !Array.isArray(firstValue)) {
        const objectSchema: SingerSchema = { type: ["object", "null"], properties: {} };
        for (const [k, v] of Object.entries(firstValue)) {
          if (objectSchema.properties) {
            objectSchema.properties[k] = toSingerSchema(v);
          }
        }
        headerMap.properties[col] = objectSchema;
      } else {
        headerMap.properties[col] = TYPE_MAPPING.str;
      }
    } else {
      headerMap.properties[col] = TYPE_MAPPING.str;

      // Serialize complex objects to strings for Singer compatibility
      const values = df.getColumn(col).toArray();
      const hasComplexObjects = values.some(v =>
        v != null && (typeof v === 'object' || Array.isArray(v))
      );
      
      if (hasComplexObjects) {
        try {
          const serializedValues = values.map((value: any) => {
            if (value == null) return value;
            if (typeof value === 'object' || Array.isArray(value)) {
              return JSON.stringify(deepConvertDatetimes(value));
            }
            return value;
          });
          
          modifiedDf = modifiedDf.withColumn(
            pl.lit(serializedValues).alias(col)
          );
        } catch (error) {
          console.warn(`Failed to serialize complex objects in column ${col}:`, error);
        }
      }
    }
  }

  // Update schema using types from catalog and keeping extra columns
  if (catalogSchema && schema) {
    Object.assign(headerMap.properties, schema.properties);
  }

  return [modifiedDf, headerMap];
}

export function toSingerSchema(input: any): SingerSchema {
  if (typeof input === 'object' && input !== null && !Array.isArray(input)) {
    const property: SingerSchema = { type: ["object", "null"], properties: {} };
    for (const [k, v] of Object.entries(input)) {
      if (property.properties) {
        property.properties[k] = toSingerSchema(v);
      }
    }
    return property;
  } else if (Array.isArray(input)) {
    if (input.length > 0) {
      return { type: ["array", "null"], items: toSingerSchema(input[0]) };
    } else {
      return { items: { type: ["string", "null"] }, type: ["array", "null"] };
    }
  } else if (typeof input === 'boolean') {
    return { type: ["boolean", "null"] };
  } else if (typeof input === 'number') {
    return Number.isInteger(input) 
      ? { type: ["integer", "null"] }
      : { type: ["number", "null"] };
  }
  return { type: ["string", "null"] };
}

export function deepConvertDatetimes(value: any): any {
  if (Array.isArray(value)) {
    return value.map(child => deepConvertDatetimes(child));
  } else if (typeof value === 'object' && value !== null) {
    const result: any = {};
    for (const [k, v] of Object.entries(value)) {
      result[k] = deepConvertDatetimes(v);
    }
    return result;
  } else if (value instanceof Date) {
    return value.toISOString();
  }
  return value;
}

export function parseObjs(x: any): any {
  if (typeof x !== 'string') {
    return x;
  }

  try {
    return JSON.parse(x);
  } catch (error) {
    // If JSON parsing fails, return the original string
    return x;
  }
}

export function getCatalogSchema(stream: string): SingerHeaderMap {
  const reader = new Reader();
  const catalog = reader['readCatalog'](); // Access private method
  
  if (!catalog) {
    throw new Error(`No catalog found`);
  }

  const streamInfo = catalog.streams.find(
    s => s.stream === stream || s.tap_stream_id === stream
  );

  if (!streamInfo) {
    throw new Error(`No schema found in catalog for stream ${stream}`);
  }

  const schema = streamInfo.schema;
  const relevantSchema = {
    type: (schema as any).type || ["object", "null"],
    properties: schema.properties || {}
  };

  // Ensure every array type has an items dict
  for (const [propName, prop] of Object.entries(relevantSchema.properties)) {
    if (prop && typeof prop === 'object') {
      const propType = prop.type;
      if ((propType === "array" || (Array.isArray(propType) && propType.includes("array"))) && !prop.items) {
        prop.items = {};
      }
    }
  }

  return relevantSchema as SingerHeaderMap;
}

export function parseDfCols(df: pl.DataFrame, schema: SingerHeaderMap): pl.DataFrame {
  let modifiedDf = df.clone();

  for (const col of df.columns) {
    const colType = schema.properties[col]?.type || [];
    const typeArray = Array.isArray(colType) ? colType : [colType];
    
    // Check if column should contain objects or arrays
    const shouldParseObjects = typeArray.some(type => ["object", "array"].includes(type));
    
    if (shouldParseObjects) {
      try {
        const values = df.getColumn(col).toArray();
        const parsedValues = values.map((value: any) => parseObjs(value));
        
        modifiedDf = modifiedDf.withColumn(
          pl.lit(parsedValues).alias(col)
        );
      } catch (error) {
        console.warn(`Failed to parse objects in column ${col}:`, error);
      }
    }
  }

  return modifiedDf;
}

export function toSinger(
  df: pl.DataFrame,
  stream: string,
  outputDir: string,
  options: {
    keys?: string[];
    filename?: string;
    allowObjects?: boolean;
    schema?: SingerHeaderMap;
    keepNullFields?: boolean;
    catalogStream?: string;
    recursiveTyping?: boolean;
  } = {}
): void {
  const {
    keys = [],
    filename = "data.singer",
    allowObjects = false,
    schema,
    keepNullFields = false,
    catalogStream,
    recursiveTyping = true
  } = options;

  const catalogSchema = process.env.USE_CATALOG_SCHEMA?.toLowerCase() === "true";
  
  let processedDf = df.clone();
  let finalSchema = schema;

  // Drop columns with all null values unless we want to keep null fields
  if (allowObjects && !catalogSchema && !keepNullFields) {
    // Filter out columns that are entirely null
    const columnsToKeep = df.columns.filter(col => {
      const values = df.getColumn(col).toArray();
      return values.some(v => v != null);
    });
    if (columnsToKeep.length > 0) {
      // processedDf = processedDf.select(columnsToKeep);
    }
  }

  if (catalogSchema || catalogStream) {
    const streamName = catalogStream || stream;
    finalSchema = getCatalogSchema(streamName);
    processedDf = parseDfCols(processedDf, finalSchema);
  }

  const [modifiedDf, headerMap] = genSingerHeader(
    processedDf,
    allowObjects || catalogSchema,
    finalSchema,
    catalogSchema,
    recursiveTyping
  );

  // Ensure output directory exists
  fs.ensureDirSync(outputDir);
  
  const outputPath = path.join(outputDir, filename);
  const mode = fs.existsSync(outputPath) ? 'a' : 'w';

  // Write singer format
  const schemaRecord = {
    type: "SCHEMA",
    stream: stream,
    schema: headerMap,
    key_properties: keys
  };

  let output = '';
  output += JSON.stringify(schemaRecord) + '\n';

  // Convert DataFrame to records and write each as a RECORD
  const records = modifiedDf.toRecords();
  for (const record of records) {
    let filteredRecord = record;
    
    // Filter null values unless we want to keep them
    if (!catalogSchema && !keepNullFields) {
      filteredRecord = Object.fromEntries(
        Object.entries(record).filter(([_, value]) => value != null)
      );
    }

    // Convert datetimes
    filteredRecord = deepConvertDatetimes(filteredRecord);

    const singerRecord = {
      type: "RECORD",
      stream: stream,
      record: filteredRecord
    };

    output += JSON.stringify(singerRecord) + '\n';
  }

  // Write state record
  const stateRecord = {
    type: "STATE",
    value: {}
  };
  output += JSON.stringify(stateRecord) + '\n';

  // Write or append to file
  if (mode === 'a') {
    fs.appendFileSync(outputPath, output);
  } else {
    fs.writeFileSync(outputPath, output);
  }
}

function getColumnType(dtype: string): string | null {
  if (dtype.includes('date') || dtype.includes('time')) {
    return 'date';
  } else if (dtype.includes('float') || dtype.includes('double')) {
    return 'float';
  } else if (dtype.includes('int')) {
    return 'int';
  } else if (dtype.includes('bool')) {
    return 'bool';
  } else if (dtype.includes('str') || dtype.includes('utf8')) {
    return 'str';
  }
  return null;
}
