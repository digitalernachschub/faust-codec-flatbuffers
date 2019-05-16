import json
import string
import subprocess
import tempfile
import types
from keyword import iskeyword
from pathlib import Path
from typing import Any, Mapping, NamedTuple, Sequence, Type

import faust
from hypothesis import assume, given, settings, HealthCheck
from hypothesis.strategies import binary, composite, data, floats, integers, just, lists, sampled_from, text

from faust_codec_flatbuffers.codec import FlatbuffersCodec
from faust_codec_flatbuffers.faust_model_converter import Float64, UInt8, Int8, UInt16, Int16, UInt32, Int64, UInt64


@composite
def python_identifier(draw):
    identifier = draw(text(alphabet=string.ascii_letters, min_size=1))
    assume(not iskeyword(identifier))
    return identifier


_strategies_by_field_type = {
    str: text(),
    UInt8: integers(min_value=0, max_value=2**8-1),
    Int8: integers(min_value=-2**7, max_value=2**7-1),
    UInt16: integers(min_value=0, max_value=2**16-1),
    Int16: integers(min_value=-2**15, max_value=2**15-1),
    int: integers(min_value=-2**31, max_value=2**31-1),
    UInt32: integers(min_value=0, max_value=2**32-1),
    Int64: integers(min_value=-2**63, max_value=2**63-1),
    UInt64: integers(min_value=0, max_value=2**64-1),
    # NaN will break equality tests, because float('nan') != float('nan')
    float: floats(width=32, allow_nan=False),
    Float64: floats(allow_nan=False),
}


_scalar_field_type = sampled_from(list(_strategies_by_field_type.keys()))


@composite
def _container_field_type(draw):
    container_type = draw(sampled_from([Sequence]))
    element_type = draw(_scalar_field_type)
    return container_type[element_type]


_field_type = _scalar_field_type | just(bytes) | _container_field_type()


def _strategy_by_field_type(field_type: Type):
    type_name = getattr(field_type, '_name', str(field_type))
    if 'Sequence' in type_name:
        element_type = field_type.__args__[0]
        if element_type == UInt8 or element_type == Int8:
            return binary()
        return lists(_strategy_by_field_type(element_type))
    elif field_type == bytes:
        return binary()
    return _strategies_by_field_type[field_type]


@composite
def model_field(draw, name=python_identifier(), type_=_field_type):
    return draw(name), draw(type_)


@composite
def model_type(draw, fields=lists(model_field()), include_metadata=just(True)):
    fields = {name: type_ for name, type_ in draw(fields)}
    model_type = types.new_class(
        'Data',
        bases=(faust.Record,),
        kwds={'include_metadata': draw(include_metadata)},
        exec_body=lambda class_vars: class_vars.update({'__annotations__': fields})
    )
    return model_type


@composite
def model(draw, model_class=model_type()):
    type_ = draw(model_class)
    model_args = {}
    for field_name, field_type in type_._options.fields.items():
        model_args[field_name] = draw(_strategy_by_field_type(field_type))
    model = type_(**model_args)
    return model


def _to_binary_schema(definition: str) -> bytes:
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


def _reference_deserialize(definition: str, data: bytes) -> Mapping[str, Any]:
    with tempfile.TemporaryDirectory() as output_dir:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fbs') as schema_definition_file:
            schema_definition_file.write(definition)
            schema_definition_file.flush()
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.bin') as data_file:
                data_file.write(data)
                data_file.flush()
                subprocess.run(['flatc', '--json', '--strict-json', '--raw-binary', '-o', output_dir, schema_definition_file.name, '--', data_file.name], check=True)
        deserialized_data_files = list(Path(output_dir).glob('**/*.json'))
        if len(deserialized_data_files) > 1:
            deserialized_data_paths = [str(f) for f in deserialized_data_files]
            raise ValueError('More than one Flatbuffers file found: ' + ', '.join(deserialized_data_paths))
        with open(str(deserialized_data_files[0]), 'r') as f:
            return json.load(f)


class Field(NamedTuple):
    name: str
    type: Type


class Table(NamedTuple):
    name: str
    fields: Sequence[Field]


@composite
def field(draw):
    name = draw(text(alphabet=string.ascii_letters, min_size=1))
    type_ = draw(sampled_from(['string', 'int']))
    return Field(name=name, type=type_)


@composite
def table(draw, name=text(alphabet=string.ascii_letters, min_size=1)):
    return Table(
        name=draw(name),
        fields=draw(lists(field(), unique_by=lambda f: f.name)),
    )


_model_field_type_by_flatbuffers_type = {
    'string': str,
    'int': int,
}


def _to_schema_definition(table: Table) -> str:
    schema_definition = f'table {table.name} {{'
    for f in table.fields:
        schema_definition += f'{f.name}:{f.type};\n'
    schema_definition += '}\n'
    schema_definition += f'root_type {table.name};'
    return schema_definition


def _to_faust_model_type(table: Table) -> Type:
    fields = {f.name: _model_field_type_by_flatbuffers_type[f.type] for f in table.fields}
    model_type = types.new_class(
        table.name,
        bases=(faust.Record,),
        kwds={'include_metadata': False},
        exec_body=lambda class_vars: class_vars.update({'__annotations__': fields})
    )
    return model_type


class Data(faust.Record, include_metadata=False):
    id: str
    number: int


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(data())
def test_deserialization_reverts_serialization_when_codec_is_created_from_schema(data):
    table_ = data.draw(table())
    schema_definition = _to_schema_definition(table_)

    model_type = _to_faust_model_type(table_)
    model_instance = data.draw(model(model_class=just(model_type)))
    schema = _to_binary_schema(schema_definition)
    codec = FlatbuffersCodec.from_schema(schema)
    data = model_instance.to_representation()

    data_deserialized = codec.loads(codec.dumps(data))

    assert data_deserialized == data


def test_dumps():
    schema_definition = '''
        table Data {
            id:string;
            number:int;
        }
        root_type Data;'''
    model = Data(id='abcd', number=1234)
    codec = FlatbuffersCodec.from_model(Data)
    data = model.to_representation()

    binary = codec.dumps(data)

    deserialized = _reference_deserialize(schema_definition, binary)
    assert deserialized == data


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(model())
def test_deserialization_reverts_serialization(model):
    codec = FlatbuffersCodec.from_model(type(model))
    data = model.to_representation()

    data_deserialized = codec.loads(codec.dumps(data))

    assert data_deserialized == data


def test_loads():
    model = Data(id='abcd', number=1234)
    codec = FlatbuffersCodec.from_model(Data)
    serialized = b'\x0c\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x04\x00\x08\x00\x00\x00\xd2\x04\x00\x00\x04\x00\x00\x00\x04\x00\x00\x00abcd\x00\x00\x00\x00'

    data = codec.loads(serialized)

    assert data == model.to_representation()
