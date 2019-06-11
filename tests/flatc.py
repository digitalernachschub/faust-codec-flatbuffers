import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Mapping, NewType


SchemaDefinition = NewType('SchemaDefinition', str)


def serialize_schema_definition(definition: SchemaDefinition) -> bytes:
    with tempfile.TemporaryDirectory() as output_dir:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fbs') as schema_definition_file:
            schema_definition_file.write(definition)
            schema_definition_file.flush()
            subprocess.run(['flatc', '--schema', '--binary', '-o', output_dir, schema_definition_file.name], check=True)
        binary_schema_files = list(Path(output_dir).glob('**/*.bfbs'))
        if len(binary_schema_files) > 1:
            binary_schema_paths = [str(f) for f in binary_schema_files]
            raise ValueError('More than one Flatbuffers binary schema found: ' + ', '.join(binary_schema_paths))
        with open(str(binary_schema_files[0]), 'rb') as f:
            return f.read()


def serialize(definition: SchemaDefinition, data: Mapping[str, Any]) -> bytes:
    with tempfile.TemporaryDirectory() as output_dir:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fbs') as schema_definition_file:
            schema_definition_file.write(definition)
            schema_definition_file.flush()
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as data_file:
                json.dump(data, data_file)
                data_file.flush()
                subprocess.run(['flatc', '--binary', '--raw-binary',
                                '-o', output_dir, schema_definition_file.name, data_file.name], check=True)
        serialized_data_files = list(Path(output_dir).glob('**/*.bin'))
        if len(serialized_data_files) > 1:
            serialized_data_paths = [str(f) for f in serialized_data_files]
            raise ValueError('More than one Flatbuffers file found: ' + ', '.join(serialized_data_paths))
        with open(str(serialized_data_files[0]), 'rb') as f:
            return f.read()


def deserialize(definition: SchemaDefinition, data: bytes) -> Mapping[str, Any]:
    with tempfile.TemporaryDirectory() as output_dir:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fbs') as schema_definition_file:
            schema_definition_file.write(definition)
            schema_definition_file.flush()
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.bin') as data_file:
                data_file.write(data)
                data_file.flush()
                subprocess.run(['flatc', '--json', '--strict-json', '--defaults-json', '--raw-binary',
                                '-o', output_dir, schema_definition_file.name, '--', data_file.name], check=True)
        deserialized_data_files = list(Path(output_dir).glob('**/*.json'))
        if len(deserialized_data_files) > 1:
            deserialized_data_paths = [str(f) for f in deserialized_data_files]
            raise ValueError('More than one Flatbuffers file found: ' + ', '.join(deserialized_data_paths))
        with open(str(deserialized_data_files[0]), 'r') as f:
            return json.load(f, parse_float=lambda s: round(float(s), 6))
