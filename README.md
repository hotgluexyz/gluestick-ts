# Gluestick TypeScript

A powerful TypeScript library for data processing and ETL operations on the [hotglue](https://hotglue.com) IPaaS platform, built with Polars for high-performance data manipulation. Supports multiple export formats including CSV, JSON, Parquet, and Singer specification.

## Installation

```bash
npm install @hotglue/gluestick-ts
```

[![npm version](https://badge.fury.io/js/@hotglue%2Fgluestick-ts.svg)](https://www.npmjs.com/package/@hotglue/gluestick-ts)

## Quick Start

```typescript
import * as gs from '@hotglue/gluestick-ts';

// Create a Reader to access your data
const reader = new gs.Reader();

// Get available data streams
const streams = reader.keys();
console.log('Available streams:', streams);

// Read and process a specific stream
const dataFrame = reader.get('your_stream_name', { catalogTypes: true });

// Export processed data (defaults to CSV)
gs.toExport(dataFrame, 'output_name', './output');

// Export as Singer format
gs.toExport(dataFrame, 'output_name', './output', { exportFormat: 'singer' });
```

## Core Components

### Reader Class

The `Reader` class is your main interface for accessing data streams:

```typescript
const reader = new gs.Reader(inputDir?, rootDir?);
```

**Methods:**
- `get(stream, options)` - Read a specific stream as a Polars DataFrame
- `keys()` - Get all available stream names
- `getPk(stream)` - Get primary keys for a stream from catalog

**Options:**
- `catalogTypes: boolean` - Use catalog for automatic type inference
- Other options will be passed through to Polars when reading. See [ReadCSV](https://pola-rs.github.io/nodejs-polars/interfaces/ReadCsvOptions.html) and [ReadParquet](https://pola-rs.github.io/nodejs-polars/interfaces/ReadParquetOptions.html) options for more information
### Export Functions

Export your processed data in multiple formats:

```typescript
gs.toExport(dataFrame, outputName, outputDir, options?);
```

**Supported formats:**
- **CSV** (default) - Comma-separated values
- **JSON** - Single JSON array
- **JSONL** - Newline-delimited JSON
- **Parquet** - Columnar storage format
- **Singer** - Singer specification format for data integration

## Development

Build the project:

```bash
npm run build
```

Run examples:

```bash
# Run CSV processing example
npm run run:example:csv

# Run Parquet processing example  
npm run run:example:parquet
```

## API Reference

### Reader Constructor

```typescript
new Reader(inputDir?: string, rootDir?: string)
```

- `inputDir` - Custom input directory (default: `${rootDir}/sync-output`)
- `rootDir` - Root directory (default: `process.env.ROOT_DIR || '.'`)

### Reader Methods

#### `get(stream: string, options?: ReadOptions): DataFrame | null`

Read a data stream as a Polars DataFrame.

```typescript
const df = reader.get('users', { catalogTypes: true });
```

**Options:**
- `catalogTypes: boolean` - Use catalog for automatic type inference

#### `keys(): string[]`

Get all available stream names.

```typescript
const streams = reader.keys();
// Returns: ['users', 'orders', 'products']
```

#### `getPk(stream: string): string[] | null`

Get primary keys for a stream from the catalog.

```typescript
const primaryKeys = reader.getPk('users');
// Returns: ['id']
```

### Export Function

```typescript
toExport(
  dataFrame: DataFrame,
  outputName: string,
  outputDir: string,
  options?: ExportOptions
): void
```

**Parameters:**
- `dataFrame` - Polars DataFrame to export
- `outputName` - Name for the output file (without extension)
- `outputDir` - Directory to write the output file
- `options` - Export configuration options

**Export Options:**
```typescript
interface ExportOptions {
  exportFormat?: 'csv' | 'json' | 'jsonl' | 'parquet' | 'singer';
  outputFilePrefix?: string;
  keys?: string[];  // Primary keys for the data
  stringifyObjects?: boolean;
  reservedVariables?: Record<string, string>;
  allowObjects?: boolean;  // For Singer format
  schema?: SingerHeaderMap;  // For Singer format
}
```

**Examples:**
```typescript
// Export as CSV with prefix
gs.toExport(dataFrame, 'processed_users', './output', {
  exportFormat: 'csv',
  outputFilePrefix: 'tenant_123_',
  keys: ['user_id']
});

// Export as Singer format
gs.toExport(dataFrame, 'processed_users', './output', {
  exportFormat: 'singer',
  allowObjects: true,
  keys: ['user_id']
});
```

## Singer Format Support

Export data in [Singer specification](https://hub.meltano.com/singer/spec) format for data integration pipelines:

```typescript
// Basic Singer export
gs.toExport(dataFrame, 'users', './output', {
  exportFormat: 'singer',
  keys: ['id']
});

// Singer export with object support
gs.toExport(dataFrame, 'users', './output', {
  exportFormat: 'singer',
  allowObjects: true,
  keys: ['id']
});
```

The Singer export automatically generates SCHEMA, RECORD, and STATE messages according to the Singer specification.
