import fs from 'fs-extra';
import * as path from 'path';
import pl from 'nodejs-polars';

export interface CatalogStream {
  stream: string;
  tap_stream_id: string;
  schema: {
    properties: Record<string, any>;
  };
  metadata: Array<{
    breadcrumb: string[];
    metadata: Record<string, any>;
  }>;
}

export interface Catalog {
  streams: CatalogStream[];
}

export interface PolarsSchema {
  [column: string]: any;
}

export class Reader {
  private static readonly ROOT_DIR = process.env.ROOT_DIR || '.';
  private static readonly INPUT_DIR = `${Reader.ROOT_DIR}/sync-output`;

  private root: string;
  private dir: string;
  private inputFiles: Record<string, string>;

  constructor(dir: string = Reader.INPUT_DIR, root: string = Reader.ROOT_DIR) {
    this.root = root;
    this.dir = dir;
    this.inputFiles = this.readDirectories();
  }

  toString(): string {
    return JSON.stringify(Object.keys(this.inputFiles));
  }

  keys(): string[] {
    return Object.keys(this.inputFiles);
  }

  get(stream: string, options: {
    catalogTypes?: boolean;
    chunksize?: number;
    [key: string]: any;
  } = {}): pl.DataFrame | null {
    const filepath = this.inputFiles[stream];
    if (!filepath) {
      return null;
    }

    // Since we're excluding parquet support for now, only handle CSV files
    if (filepath.endsWith('.csv')) {
      const catalog = this.readCatalog();
      let readOptions: any = { ...options };

      if (catalog && options.catalogTypes) {
        const schema = this.getSchemaFromCatalog(catalog, stream);
        if (Object.keys(schema).length > 0) {
          readOptions.dtype = schema;
        }
      }

      try {
        let df = pl.readCSV(filepath, readOptions);

        // Handle date parsing if specified
        if (readOptions.parseDates) {
          for (const dateCol of readOptions.parseDates) {
            try {
              df = df.withColumn(
                pl.col(dateCol).str.strptime(pl.Datetime('us'), '%Y-%m-%d %H:%M:%S%.f')
              );
            } catch (e) {
              // Try alternative date format
              try {
                df = df.withColumn(
                  pl.col(dateCol).str.strptime(pl.Datetime('us'), '%Y-%m-%d')
                );
              } catch (e2) {
                console.warn(`Failed to parse dates for column ${dateCol}: ${e2}`);
              }
            }
          }
        }

        return df;
      } catch (error) {
        console.error(`Failed to read CSV file ${filepath}:`, error);
        return null;
      }
    }

    return null;
  }

  getPk(stream: string): string[] {
    const keyProperties: string[] = [];
    const catalog = this.readCatalog();

    if (catalog) {
      const streamInfo = catalog.streams.find(
        (c) => c.stream === stream || c.tap_stream_id === stream
      );

      if (streamInfo && streamInfo.metadata) {
        const breadcrumb = streamInfo.metadata.find(
          (s) => s.breadcrumb.length === 0
        );

        if (breadcrumb && breadcrumb.metadata) {
          const tableKeyProperties = breadcrumb.metadata['table-key-properties'];
          if (Array.isArray(tableKeyProperties)) {
            keyProperties.push(...tableKeyProperties);
          }
        }
      }
    }

    return keyProperties;
  }

  private readDirectories(ignore: string[] = []): Record<string, string> {
    const results: Record<string, string> = {};
    let allFiles: string[] = [];

    if (fs.existsSync(this.dir) && fs.statSync(this.dir).isDirectory()) {
      const entries = fs.readdirSync(this.dir);
      for (const entry of entries) {
        const filePath = path.join(this.dir, entry);
        if (fs.statSync(filePath).isFile()) {
          if (filePath.endsWith('.csv')) {
            allFiles.push(filePath);
          }
        }
      }
    } else if (fs.existsSync(this.dir)) {
      allFiles.push(this.dir);
    }

    for (const file of allFiles) {
      const filename = path.basename(file);
      let entityType = filename.replace(/\.(csv|parquet)$/, '');

      if (entityType.includes('-')) {
        entityType = entityType.split('-')[0];
      }

      if (!results[entityType] && !ignore.includes(entityType)) {
        results[entityType] = file;
      }
    }

    return results;
  }

  private readCatalog(): Catalog | null {
    const catalogPath = path.join(this.root, 'catalog.json');

    if (fs.existsSync(catalogPath)) {
      try {
        const catalogData = fs.readFileSync(catalogPath, 'utf8');
        return JSON.parse(catalogData) as Catalog;
      } catch (error) {
        console.warn(`Failed to read catalog: ${error}`);
        return null;
      }
    }

    return null;
  }

  private getSchemaFromCatalog(catalog: Catalog, stream: string): PolarsSchema {
    const filepath = this.inputFiles[stream];
    if (!filepath) {
      return {};
    }

    // Get headers from CSV
    let headers: string[] = [];
    try {
      const df = pl.readCSV(filepath, { nRows: 0 });
      headers = df.columns;
    } catch (error) {
      console.warn(`Failed to read headers from ${filepath}:`, error);
      return {};
    }

    const streamInfo = catalog.streams.find(
      (c) => c.stream === stream || c.tap_stream_id === stream
    );

    if (!streamInfo) {
      return {};
    }

    const types = streamInfo.schema.properties;
    const schema: PolarsSchema = {};

    for (const col of headers) {
      const colType = types[col];
      if (colType) {
        // Handle anyOf types
        const anyOfList = colType.anyOf || [];
        let finalColType = colType;

        if (anyOfList.length > 0) {
          const typeWithFormat = anyOfList.find((t: any) => t.format);
          finalColType = typeWithFormat || { type: 'object' };
        }

        if (finalColType.format === 'date-time') {
          schema[col] = pl.Datetime;
          continue;
        }

        if (finalColType.type) {
          const catalogType = Array.isArray(finalColType.type)
            ? finalColType.type.filter((t: string) => t !== 'null')
            : [finalColType.type];

          if (catalogType.length === 1) {
            switch (catalogType[0]) {
              case 'integer':
                schema[col] = pl.Int64;
                break;
              case 'number':
                schema[col] = pl.Float64;
                break;
              case 'boolean':
                schema[col] = pl.Bool;
                break;
              default:
                schema[col] = pl.Utf8;
                break;
            }
            continue;
          }
        }
      }
      schema[col] = pl.Utf8;
    }

    return schema;
  }
}