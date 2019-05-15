from typing import Any, Collection, Mapping, Type

import faust
import flatbuffers

from faust_codec_flatbuffers.faust_model_converter import to_flatbuffers_schema
from faust_codec_flatbuffers.reflection import Schema
from faust_codec_flatbuffers.reflection import Object
from faust_codec_flatbuffers.reflection import Field
from faust_codec_flatbuffers.reflection.BaseType import BaseType
from faust_codec_flatbuffers.reflection.Type import Type as FieldType


_number_type_by_base_type = {
    BaseType.UByte: flatbuffers.number_types.Uint8Flags,
    BaseType.Byte: flatbuffers.number_types.Int8Flags,
    BaseType.UShort: flatbuffers.number_types.Uint16Flags,
    BaseType.Short: flatbuffers.number_types.Int16Flags,
    BaseType.UInt: flatbuffers.number_types.Uint32Flags,
    BaseType.Int: flatbuffers.number_types.Int32Flags,
    BaseType.ULong: flatbuffers.number_types.Uint64Flags,
    BaseType.Long: flatbuffers.number_types.Int64Flags,
    BaseType.Float: flatbuffers.number_types.Float32Flags,
    BaseType.Double: flatbuffers.number_types.Float64Flags,
    BaseType.String: flatbuffers.number_types.UOffsetTFlags,
}


class FlatbuffersCodec(faust.Codec):
    def __init__(self, model: Type[faust.Record]):
        super().__init__()
        self.faust_metadata = {'ns': model._options.namespace} if model._options.include_metadata else None
        self.schema = to_flatbuffers_schema(model)

    def _loads(self, binary: bytes) -> Mapping[str, Any]:
        model_descriptor = self.schema.RootTable()
        output_fields = [model_descriptor.Fields(field_index) for field_index in range(model_descriptor.FieldsLength())]
        output_fields.sort(key=lambda field: field.Offset())

        root_table_offset = flatbuffers.encode.Get(flatbuffers.packer.uoffset, binary, 0)
        table = flatbuffers.Table(binary, root_table_offset)
        model_data = {}
        for field in output_fields:
            field_name = field.Name().decode('utf-8')
            model_data[field_name] = FlatbuffersCodec._get_field(table, field)
        if self.faust_metadata:
            model_data['__faust'] = self.faust_metadata
        return model_data

    @staticmethod
    def _get_field(table: flatbuffers.Table, field: Field):
        offset = flatbuffers.number_types.UOffsetTFlags.py_type(table.Offset(field.Offset()))
        if offset == 0:
            return field.DefaultInteger()
        field_type = field.Type().BaseType()
        if field_type == BaseType.String:
            value = table.String(offset + table.Pos).decode('utf-8')
        elif field_type == BaseType.Vector:
            element_type = field.Type().Element()
            if element_type == BaseType.UByte or element_type == BaseType.Byte:
                value = table.GetVectorAsNumpy(flatbuffers.number_types.Uint8Flags, offset).tobytes()
            else:
                vector_offset = table.Vector(offset)
                element_count = table.VectorLen(offset)
                element_size = _number_type_by_base_type[element_type].bytewidth
                value = []
                for index in range(element_count):
                    element_offset = vector_offset+index*element_size
                    if element_type == BaseType.String:
                        value.append(table.String(element_offset).decode('utf-8'))
                    else:
                        value.append(table.Get(_number_type_by_base_type[element_type], element_offset))
        elif field_type in _number_type_by_base_type:
            value = table.Get(_number_type_by_base_type[field_type], offset + table.Pos)
        else:
            raise NotImplementedError(f'Unsupported field type: {field_type}')
        return value

    def _dumps(self, record: Mapping[str, Any]) -> bytes:
        return self._encode_schema(record, self.schema)

    def _encode_schema(self, data: Mapping[str, Any], schema: Schema) -> bytes:
        root_object = schema.RootTable()
        return self._encode_object(data, root_object)

    def _encode_object(self, data: Mapping[str, Any], object_: Object):
        input_fields = [object_.Fields(field_index) for field_index in range(object_.FieldsLength())]
        input_fields.sort(key=lambda field: field.Offset())
        builder = flatbuffers.Builder(1024)
        encoded_fields = []
        for slot, field in enumerate(input_fields):
            field_type = field.Type().BaseType()
            name = field.Name().decode('utf-8')
            value = data.get(name)
            if value is None:
                continue
            encoded_value = self._encode_field(builder, value, field.Type())
            encoded_fields.append((field_type, slot, encoded_value))
        builder.StartObject(len(input_fields))
        for field_type, slot, encoded_value in encoded_fields:
            if field_type == BaseType.String:
                builder.PrependUOffsetTRelativeSlot(slot, flatbuffers.number_types.UOffsetTFlags.py_type(encoded_value), 0)
            elif field_type == BaseType.Vector:
                builder.PrependUOffsetTRelativeSlot(slot, flatbuffers.number_types.UOffsetTFlags.py_type(encoded_value), 0)
            elif field_type in _number_type_by_base_type:
                builder.PrependSlot(_number_type_by_base_type[field_type], slot, encoded_value, 0)
            else:
                raise NotImplementedError('Unsupported field type %s' % field_type)
        object_encoded = builder.EndObject()
        builder.Finish(object_encoded)
        return bytes(builder.Output())

    def _encode_field(self, builder: flatbuffers.Builder, value: Any, type_: FieldType) -> int:
        base_type = type_.BaseType()
        if base_type == BaseType.Vector:
            return self._encode_vector(builder, value, element_type=type_.Element())
        return self._encode_value(builder, value, base_type)

    def _encode_value(self, builder: flatbuffers.Builder, value: Any, type_: BaseType) -> int:
        if type_ == BaseType.String:
            return builder.CreateString(value)
        elif type_ in _number_type_by_base_type.keys():
            return value
        raise NotImplementedError(f'Unsupported field type: {type_}')

    def _encode_vector(self, builder: flatbuffers.Builder, value: Collection, element_type: BaseType) -> int:
        if element_type == BaseType.UByte or element_type == BaseType.Byte:
            return builder.CreateByteVector(value)
        elements_encoded = [self._encode_value(builder, element, element_type) for element in value]
        element_bytesize = _number_type_by_base_type[element_type].bytewidth
        vector_element_type = _number_type_by_base_type[element_type]
        builder.StartVector(element_bytesize, len(elements_encoded), element_bytesize)
        for index, element in enumerate(reversed(elements_encoded)):
            if vector_element_type == flatbuffers.number_types.UOffsetTFlags:
                builder.PrependUOffsetTRelative(element)
            else:
                builder.Prepend(_number_type_by_base_type[element_type], element)
        return builder.EndVector(len(elements_encoded))
