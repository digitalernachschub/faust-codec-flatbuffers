import string
from keyword import iskeyword

import faust
from hypothesis import assume, given
from hypothesis.strategies import composite, dictionaries, sampled_from, text

from faust_codec_flatbuffers.codec import FlatbuffersCodec


@composite
def python_identifier(draw):
    identifier = draw(text(alphabet=string.ascii_letters, min_size=1))
    assume(not iskeyword(identifier))
    return identifier


_model_fields = dictionaries(python_identifier(), sampled_from([str, int]))


@composite
def model(draw):
    fields = draw(_model_fields)
    model_type = type('Data', (faust.Record,), {'__annotations__': fields})
    model_args = {}
    for field_name, field_type in fields.items():
        model_args[field_name] = 'abcd' if field_type == str else 1234
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