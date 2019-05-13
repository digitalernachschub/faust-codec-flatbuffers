import string
from keyword import iskeyword

import faust
from hypothesis import assume, given
from hypothesis.strategies import composite, dictionaries, integers, sampled_from, text

from faust_codec_flatbuffers.codec import FlatbuffersCodec
from faust_codec_flatbuffers.faust_model_converter import UInt8, Int8, UInt16, Int16, UInt32, Int64, UInt64


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
}
_model_fields = dictionaries(python_identifier(), sampled_from(list(_strategies_by_field_type.keys())))


@composite
def model(draw):
    fields = draw(_model_fields)
    model_type = type('Data', (faust.Record,), {'__annotations__': fields})
    model_args = {}
    for field_name, field_type in fields.items():
        model_args[field_name] = draw(_strategies_by_field_type[field_type])
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