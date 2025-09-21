# Google Cloud Storage Uploader

A comprehensive Python tool for uploading files to Google Cloud Storage buckets with support for single file uploads, batch directory uploads, and concurrent processing.

## Features

- **Single File Upload**: Upload individual files to GCS buckets
- **Directory Upload**: Upload entire directories with recursive file discovery
- **Concurrent Processing**: Multi-threaded uploads for better performance
- **Progress Tracking**: Visual progress bars for batch operations
- **Public Access**: Option to make uploaded files publicly accessible
- **Error Handling**: Comprehensive error handling and logging
- **CLI Interface**: Easy-to-use command-line interface

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd gcs_uploader
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up Google Cloud authentication:
   - Create a service account in Google Cloud Console
   - Download the service account key JSON file
   - Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable

## Configuration

Create a `.env` file based on `.env.example`:

```bash
cp .env.example .env
```

Edit `.env` with your Google Cloud project details:

```env
GOOGLE_CLOUD_PROJECT=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
```

## Usage

### Command Line Interface

#### Upload a single file:
```bash
python main.py upload /path/to/file.txt my-bucket-name
```

#### Upload a file with custom blob name:
```bash
python main.py upload /path/to/file.txt my-bucket-name --blob-name custom-name.txt
```

#### Upload a file and make it public:
```bash
python main.py upload /path/to/file.txt my-bucket-name --public
```

#### Upload an entire directory:
```bash
python main.py upload-dir /path/to/directory my-bucket-name
```

#### Upload directory with prefix:
```bash
python main.py upload-dir /path/to/directory my-bucket-name --prefix "uploads/"
```

#### Upload directory with custom concurrency:
```bash
python main.py upload-dir /path/to/directory my-bucket-name --max-workers 8
```

#### List accessible buckets:
```bash
python main.py list-buckets
```

### Programmatic Usage

```python
from main import GCSUploader

# Initialize uploader
uploader = GCSUploader(project_id="your-project-id")

# Upload a single file
success = uploader.upload_file("local_file.txt", "bucket-name")

# Upload a directory
results = uploader.upload_directory("local_directory", "bucket-name")

# List buckets
buckets = uploader.list_buckets()
```

## Options

### Global Options
- `--project-id`: Google Cloud project ID
- `--credentials`: Path to service account credentials JSON file
- `--verbose, -v`: Enable verbose logging

### Upload Options
- `--blob-name`: Custom name for the blob in GCS
- `--public`: Make uploaded files publicly accessible
- `--prefix`: Prefix to add to blob names (directory uploads)
- `--max-workers`: Maximum number of concurrent uploads (default: 4)

## Error Handling

The tool provides comprehensive error handling for common scenarios:
- Invalid credentials
- Bucket not found or inaccessible
- File not found
- Network errors
- Permission issues

All errors are logged with appropriate detail levels.

## Requirements

- Python 3.7+
- Google Cloud Storage client library
- Valid Google Cloud service account credentials

## License

This project is open source and available under the MIT License.
