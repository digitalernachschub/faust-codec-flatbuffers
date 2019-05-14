import string
from keyword import iskeyword
from typing import Sequence, Type

import faust
from hypothesis import assume, given, settings, HealthCheck
from hypothesis.strategies import binary, composite, dictionaries, floats, integers, lists, sampled_from, text

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
    bytes: binary()
}


_scalar_field_type = sampled_from(list(_strategies_by_field_type.keys()))
_field_type = _scalar_field_type
_model_fields = dictionaries(python_identifier(), _field_type)


def _strategy_by_field_type(field_type: Type):
    if getattr(field_type, '_name', '')  == Sequence._name:
        return lists(_strategy_by_field_type(field_type.__args__[0]))
    return _strategies_by_field_type[field_type]


@composite
def model(draw):
    fields = draw(_model_fields)
    model_type = type('Data', (faust.Record,), {'__annotations__': fields})
    model_args = {}
    for field_name, field_type in fields.items():
        model_args[field_name] = draw(_strategy_by_field_type(field_type))
    model = model_type(**model_args)
    return model


class Data(faust.Record):
    id: str
    number: int


def test_dumps():
    model = Data(id='abcd', number=1234)
    codec = FlatbuffersCodec(Data)
    data = model.to_representation()

    binary = codec.dumps(data)

    expected = b'\x0c\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x04\x00\x08\x00\x00\x00\xd2\x04\x00\x00\x04\x00\x00\x00\x04\x00\x00\x00abcd\x00\x00\x00\x00'
    assert binary == expected


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(model())
def test_deserialization_reverts_serialization(model):
    codec = FlatbuffersCodec(type(model))
    data = model.to_representation()

    data_deserialized = codec.loads(codec.dumps(data))

    assert data_deserialized == data


def test_loads():
    model = Data(id='abcd', number=1234)
    codec = FlatbuffersCodec(Data)
    serialized = b'\x0c\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x04\x00\x08\x00\x00\x00\xd2\x04\x00\x00\x04\x00\x00\x00\x04\x00\x00\x00abcd\x00\x00\x00\x00'

    data = codec.loads(serialized)

    assert data == model.to_representation()