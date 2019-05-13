import faust

from faust_codec_flatbuffers.faust_model_converter import to_flatbuffers_schema
from faust_codec_flatbuffers.reflection.BaseType import BaseType
from faust_codec_flatbuffers.reflection.Schema import Schema


class Data(faust.Record):
    id: str
    number: int


def test_schema():
    model = Data

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


def test_schema_corresponds_to_reference():
    model = Data

    schema = to_flatbuffers_schema(model)

    expected_schema = Schema.GetRootAsSchema(b'\x18\x00\x00\x00BFBS\x10\x00\x18\x00\x04\x00\x08\x00\x0c\x00\x10\x00\x00\x00\x14'
                                b'\x00\x10\x00\x00\x00\x14\x00\x00\x00\x18\x00\x00\x00\x18\x00\x00\x00\x1c\x00\x00'
                                b'\x00 \x00\x00\x00\x01\x00\x00\x00(\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                                b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x10\x00\x04'
                                b'\x00\x08\x00\x00\x00\x0c\x00\x0c\x00\x00\x00\x0c\x00\x00\x00\x14\x00\x00\x00\x01'
                                b'\x00\x00\x00\x04\x00\x00\x00Data\x00\x00\x00\x00\x02\x00\x00\x00D\x00\x00\x00\x10'
                                b'\x00\x00\x00\x0c\x00\x10\x00\x08\x00\x0c\x00\x04\x00\x06\x00\x0c\x00\x00\x00\x01'
                                b'\x00\x06\x00\x08\x00\x00\x00\x10\x00\x00\x00\x06\x00\x00\x00number\x00\x00\xd2\xff'
                                b'\xff\xff\x00\x00\x00\x07\x0c\x00\x10\x00\x08\x00\x0c\x00\x00\x00\x06\x00\x0c\x00'
                                b'\x00\x00\x00\x00\x04\x00\x08\x00\x00\x00\x14\x00\x00\x00\x02\x00\x00\x00id\x00\x00'
                                b'\x00\x00\x06\x00\x08\x00\x07\x00\x06\x00\x00\x00\x00\x00\x00\r', 0)
    assert schema.ObjectsLength() == expected_schema.ObjectsLength()
    assert schema.Objects(0).FieldsLength() == expected_schema.Objects(0).FieldsLength()
    assert schema.Objects(0).Name() == expected_schema.Objects(0).Name()
    assert schema.Objects(0).Fields(0).Type().BaseType() == expected_schema.Objects(0).Fields(0).Type().BaseType()
    assert schema.Objects(0).Fields(0).Name() == expected_schema.Objects(0).Fields(0).Name()
    assert schema.Objects(0).Fields(0).Offset() == expected_schema.Objects(0).Fields(0).Offset()
    assert schema.Objects(0).Fields(1).Offset() == expected_schema.Objects(0).Fields(1).Offset()
