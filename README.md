# Gluestick TypeScript

A powerful TypeScript library for data processing and ETL operations on the hotglue IPaaS platform, built with Polars for high-performance data manipulation.

## Installation

```bash
npm install gluestick-ts
```

## Quick Start

```typescript
import * as gs from 'gluestick-ts';

// Create a Reader to access your data
const reader = new gs.Reader();

// Get available data streams
const streams = reader.keys();
console.log('Available streams:', streams);

// Read and process a specific stream
const dataFrame = reader.get('your_stream_name', { catalogTypes: true });

// Export processed data
gs.toExport(dataFrame, 'output_name', './output');
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

## Development

Build the project:

```bash
npm run build
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
  exportFormat?: 'csv' | 'json' | 'jsonl' | 'parquet';
  outputFilePrefix?: string;
  keys?: string[];  // Primary keys for the data
}
```

**Example:**
```typescript
gs.toExport(dataFrame, 'processed_users', './output', {
  exportFormat: 'parquet',
  outputFilePrefix: 'tenant_123_',
  keys: ['user_id']
});
```
