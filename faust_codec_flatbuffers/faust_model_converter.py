from typing import Mapping, Sequence, Type

import flatbuffers
from faust.models import Model, Record
from faust_codec_flatbuffers.reflection import Schema
from faust_codec_flatbuffers.reflection import Object
from faust_codec_flatbuffers.reflection import Field
from faust_codec_flatbuffers.reflection import Type as FieldType
from faust_codec_flatbuffers.reflection.BaseType import BaseType


def to_flatbuffers_schema(model: Model) -> Schema:
    if not isinstance(model, Record):
        raise NotImplementedError('Only Records are currently supported')
    builder = flatbuffers.Builder(1024)
    fields = []
    for field_name, type_ in model._options.fields.items():
        flatbuffers_field_type = python_type_to_flatbuffers_type.get(type_)
        if not flatbuffers_field_type:
            raise NotImplementedError('No corresponding flatbuffers type for %s' % type_)
        field_type = _create_type(builder, flatbuffers_field_type)
        fields.append(_create_field(builder, field_name, field_type))
    root_object = _create_object(builder, type(model).__name__, fields)
    schema = _create_schema(builder, root_object)
    builder.Finish(schema)
    binary_schema = bytes(builder.Output())
    return Schema.Schema.GetRootAsSchema(binary_schema, 0)


python_type_to_flatbuffers_type: Mapping[Type, BaseType] = {
    int: BaseType.Int,
    str: BaseType.String
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


def _create_field(builder: flatbuffers.Builder, name: str, type_: FieldType) -> Field.Field:
    name = builder.CreateString(name)
    Field.FieldStart(builder)
    Field.FieldAddName(builder, name)
    Field.FieldAddType(builder, type_)
    return Field.FieldEnd(builder)


def _create_type(builder: flatbuffers.Builder, base_type: BaseType) -> FieldType:
    FieldType.TypeStart(builder)
    FieldType.TypeAddBaseType(builder, base_type)
    return FieldType.TypeEnd(builder)
