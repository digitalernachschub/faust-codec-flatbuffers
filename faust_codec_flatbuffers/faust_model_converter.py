from typing import Mapping, Sequence, Type

import flatbuffers
from faust.models import Model, Record
from faust_codec_flatbuffers.reflection import Schema
from faust_codec_flatbuffers.reflection import Object
from faust_codec_flatbuffers.reflection import Field
from faust_codec_flatbuffers.reflection import Type as FieldType
from faust_codec_flatbuffers.reflection.BaseType import BaseType


class UInt8(int):
    pass


class Int8(int):
    pass


class UInt16(int):
    pass


class Int16(int):
    pass


class UInt32(int):
    pass


class Int64(int):
    pass


class UInt64(int):
    pass


class Float64(float):
    pass


def to_flatbuffers_schema(model: Type[Model]) -> Schema:
    if not isinstance(model, type(Record)):
        raise NotImplementedError('Only Records are currently supported')
    builder = flatbuffers.Builder(1024)
    fields = []
    for field_index, (field_name, type_) in enumerate(model._options.fields.items()):
        flatbuffers_field_type = python_type_to_flatbuffers_type(type_)
        if not flatbuffers_field_type:
            raise NotImplementedError('No corresponding flatbuffers type for %s' % type_)
        field_type = _create_type(builder, flatbuffers_field_type)
        field_offset = 4 + 2*field_index
        fields.append(_create_field(builder, field_name, field_type, field_offset))
    root_object = _create_object(builder, model.__name__, fields)
    schema = _create_schema(builder, root_object)
    builder.Finish(schema)
    binary_schema = bytes(builder.Output())
    return Schema.Schema.GetRootAsSchema(binary_schema, 0)


def python_type_to_flatbuffers_type(type_: Type):
    if getattr(type_, '_name', '')  == Sequence._name:
        return BaseType.Vector
    return _python_type_to_flatbuffers_type.get(type_)


_python_type_to_flatbuffers_type: Mapping[Type, BaseType] = {
    UInt8: BaseType.UByte,
    Int8: BaseType.Byte,
    UInt16: BaseType.UShort,
    Int16: BaseType.Short,
    int: BaseType.Int,
    UInt32: BaseType.UInt,
    Int64: BaseType.Long,
    UInt64: BaseType.ULong,
    float: BaseType.Float,
    Float64: BaseType.Double,
    str: BaseType.String,
    bytes: BaseType.Vector,
}


def _create_schema(builder: flatbuffers.Builder, root_object: Object.Object) -> Schema.Schema:
    Schema.SchemaStartObjectsVector(builder, 1)
    builder.PrependUOffsetTRelative(root_object)
    objects = builder.EndVector(1)

    Schema.SchemaStart(builder)
    Schema.SchemaAddObjects(builder, objects)
    Schema.SchemaAddRootTable(builder, root_object)
    return Schema.SchemaEnd(builder)


def _create_object(builder: flatbuffers.Builder, name: str, fields: Sequence[Field.Field]) -> Object.Object:
    name = builder.CreateString(name)
    Object.ObjectStartFieldsVector(builder, len(fields))
    for field in reversed(fields):
        builder.PrependUOffsetTRelative(field)
    fields_vector = builder.EndVector(len(fields))

    Object.ObjectStart(builder)
    Object.ObjectAddName(builder, name)
    Object.ObjectAddFields(builder, fields_vector)
    return Object.ObjectEnd(builder)


def _create_field(builder: flatbuffers.Builder, name: str, type_: FieldType, offset: int) -> Field.Field:
    name = builder.CreateString(name)
    Field.FieldStart(builder)
    Field.FieldAddName(builder, name)
    Field.FieldAddType(builder, type_)
    Field.FieldAddOffset(builder, offset)
    return Field.FieldEnd(builder)


def _create_type(builder: flatbuffers.Builder, base_type: BaseType) -> FieldType:
    FieldType.TypeStart(builder)
    FieldType.TypeAddBaseType(builder, base_type)
    return FieldType.TypeEnd(builder)
