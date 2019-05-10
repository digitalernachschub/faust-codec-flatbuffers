import faust

from faust_codec_flatbuffers.faust_model_converter import to_flatbuffers_schema
from faust_codec_flatbuffers.reflection.BaseType import BaseType


class Data(faust.Record):
    id: str
    number: int


def test_schema():
    model = Data(id='abcd', number=1234)

    schema = to_flatbuffers_schema(model)

    assert schema.ObjectsLength() == 1
    objects = [schema.Objects(index) for index in range(schema.ObjectsLength())]
    assert schema.RootTable().Name() == objects[0].Name()
    assert schema.RootTable().FieldsLength() == 2
    fields = [schema.RootTable().Fields(index) for index in range(schema.RootTable().FieldsLength())]
    assert fields[0].Type().BaseType() == BaseType.String
    assert fields[0].Name() == b'id'
    assert fields[1].Type().BaseType() == BaseType.Int
    assert fields[1].Name() == b'number'
