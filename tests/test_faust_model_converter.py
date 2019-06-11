from typing import Mapping, Type

from hypothesis import given

from faust_codec_flatbuffers.faust_model_converter import to_flatbuffers_schema, Float64, UInt8, Int8, UInt16, Int16, UInt32, Int64, UInt64
from faust_codec_flatbuffers.reflection.BaseType import BaseType
from faust_codec_flatbuffers.reflection.Schema import Schema

from tests import flatc
from tests.test_codec import table, _to_faust_model_type, Table, Field, FlatbuffersIdlBaseType, _to_schema_definition


_faust_model_type_by_flatbuffers_type: Mapping[BaseType, Type] = {
    BaseType.UByte: UInt8,
    BaseType.Byte: Int8,
    BaseType.UShort: UInt16,
    BaseType.Short: Int16,
    BaseType.Int: int,
    BaseType.UInt: UInt32,
    BaseType.Long: Int64,
    BaseType.ULong: UInt64,
    BaseType.Float: float,
    BaseType.Double: Float64,
    BaseType.String: str,
}


@given(model=table().map(_to_faust_model_type))
def test_schema(model):
    schema = to_flatbuffers_schema(model)

    assert schema.ObjectsLength() == 1
    objects = [schema.Objects(index) for index in range(schema.ObjectsLength())]
    assert schema.RootTable().Name() == objects[0].Name()
    assert schema.RootTable().FieldsLength() == len(model._options.fields)
    fields = [schema.RootTable().Fields(index) for index in range(schema.RootTable().FieldsLength())]
    for index, field in enumerate(fields):
        field_name = field.Name().decode()
        assert field_name in model._options.fields
        model_field_type = model._options.fields[field_name]
        expected_model_field_type = _faust_model_type_by_flatbuffers_type[field.Type().BaseType()]
        assert expected_model_field_type == model_field_type


def test_schema_corresponds_to_reference():
    table = Table('Data', [Field('id', FlatbuffersIdlBaseType.STRING), Field('number', FlatbuffersIdlBaseType.INT)])
    model = _to_faust_model_type(table)

    schema = to_flatbuffers_schema(model)

    expected_schema = Schema.GetRootAsSchema(flatc.serialize_schema_definition(_to_schema_definition(table)), 0)
    assert schema == expected_schema
