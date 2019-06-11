import string
import types
from keyword import iskeyword
from typing import Any, Iterable, Mapping, NamedTuple, Sequence, Type, Union

import faust
import pytest
from hypothesis import assume, given, settings, HealthCheck
from hypothesis.strategies import binary, composite, data, floats, integers, just, lists, sampled_from, text

from faust_codec_flatbuffers.codec import FlatbuffersCodec
from faust_codec_flatbuffers.faust_model_converter import Float64, UInt8, Int8, UInt16, Int16, UInt32, Int64, UInt64
from tests import flatc


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
    #
    # Infinity will break test_dumps and test_loads, because they feed flatc with json-encoded data and JSON
    # does not support NaN or Infinity values.
    float: floats(width=32, allow_nan=False, allow_infinity=False),
    Float64: floats(width=64, allow_nan=False, allow_infinity=False),
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


_model_field_type_by_flatbuffers_type = {
    'string': str,
    'byte': Int8,
    'ubyte': UInt8,
    'short': Int16,
    'ushort': UInt16,
    'int': int,
    'uint': UInt32,
    'long': Int64,
    'ulong': UInt64,
    'int8': Int8,
    'uint8': UInt8,
    'int16': Int16,
    'uint16': UInt16,
    'int32': int,
    'uint32': UInt32,
    'int64': Int64,
    'uint64': UInt64,
    'float': float,
    'float32': float,
    'float64': Float64,
    'double': Float64,
}
_flatbuffers_primitive_types = list(_model_field_type_by_flatbuffers_type.keys())


class Field(NamedTuple):
    name: str
    type: Type


class Table(NamedTuple):
    name: str
    fields: Sequence[Field]


@composite
def field(draw):
    name = draw(python_identifier())
    type_ = draw(sampled_from(_flatbuffers_primitive_types))
    return Field(name=name, type=type_)


@composite
def table(draw, name=python_identifier()):
    name_ = draw(name)
    fields_ = draw(lists(field(), unique_by=lambda f: f.name))
    # Fields with the same name as the table are not allowed
    assume(all(f.name != name_ for f in fields_))
    return Table(name=name_, fields=fields_)


def _to_schema_definition(table: Table) -> flatc.SchemaDefinition:
    schema_definition = f'table {table.name} {{'
    for f in table.fields:
        schema_definition += f'{f.name}:{f.type};\n'
    schema_definition += '}\n'
    schema_definition += f'root_type {table.name};'
    return flatc.SchemaDefinition(schema_definition)


def _to_faust_model_type(table: Table) -> Type[faust.Record]:
    fields = {f.name: _model_field_type_by_flatbuffers_type[f.type] for f in table.fields}
    model_type = types.new_class(
        table.name,
        bases=(faust.Record,),
        kwds={'include_metadata': False},
        exec_body=lambda class_vars: class_vars.update({'__annotations__': fields})
    )
    return model_type


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(data())
def test_deserialization_reverts_serialization_when_codec_is_created_from_schema(data):
    table_ = data.draw(table())
    schema_definition = _to_schema_definition(table_)

    model_type = _to_faust_model_type(table_)
    model_instance = data.draw(model(model_class=just(model_type)))
    schema = flatc.serialize_schema_definition(schema_definition)
    codec = FlatbuffersCodec.from_schema(schema)
    data = model_instance.to_representation()

    data_deserialized = codec.loads(codec.dumps(data))

    assert data_deserialized == data


@given(data())
def test_dumps(data):
    table_ = data.draw(table())
    schema_definition = _to_schema_definition(table_)
    model_type = _to_faust_model_type(table_)
    model_instance = data.draw(model(model_class=just(model_type)))
    codec = FlatbuffersCodec.from_model(model_type)
    data_json = model_instance.to_representation()

    binary = codec.dumps(data_json)

    deserialized = flatc.deserialize(schema_definition, binary)
    # flatc must round the floats when converting to a JSON representation
    # This means we are limited to 6 digits for float and 12 digits for double
    # see https://github.com/google/flatbuffers/issues/5371
    assert deserialized == approx(data_json)


@given(data())
def test_loads(data):
    table_ = data.draw(table())
    schema_definition = _to_schema_definition(table_)
    model_type = _to_faust_model_type(table_)
    model_instance = data.draw(model(model_class=just(model_type)))
    codec = FlatbuffersCodec.from_model(model_type)
    data_json = model_instance.to_representation()
    serialized = flatc.serialize(schema_definition, data_json)

    data_deserialized = codec.loads(serialized)

    assert data_deserialized == data_json


def approx(value: Union[Any, Sequence[Any], Mapping[str, Any]], float_precision_abs=1e-6):
    if isinstance(value, Mapping):
        matcher = {k: approx(v) for k, v in value.items()}
    elif isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        matcher = [approx(v) for v in value]
    else:
        try:
            matcher = pytest.approx(value, abs=float_precision_abs)
        except:
            matcher = value
    return matcher
