from typing import Any, Mapping

import faust
import flatbuffers

from faust_codec_flatbuffers.faust_model_converter import to_flatbuffers_schema
from faust_codec_flatbuffers.reflection import Schema
from faust_codec_flatbuffers.reflection import Object
from faust_codec_flatbuffers.reflection import Field
from faust_codec_flatbuffers.reflection.BaseType import BaseType


class FlatbuffersCodec(faust.Codec):
    def __init__(self, model: faust.Record):
        super().__init__()
        self.schema = to_flatbuffers_schema(model)

    def _loads(self, binary: bytes) -> Any:
        raise NotImplementedError()

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
            encoded_value = self._encode_field(builder, value, field)
            encoded_fields.append((field_type, slot, encoded_value))
        builder.StartObject(len(input_fields))
        for field_type, slot, encoded_value in encoded_fields:
            if field_type == BaseType.UByte:
                builder.PrependUint8Slot(slot, encoded_value, 0)
            elif field_type == BaseType.UInt:
                builder.PrependUint32Slot(slot, encoded_value, 0)
            elif field_type == BaseType.Int:
                builder.PrependInt32Slot(slot, encoded_value, 0)
            elif field_type == BaseType.String:
                builder.PrependUOffsetTRelativeSlot(slot, flatbuffers.number_types.UOffsetTFlags.py_type(encoded_value), 0)
            elif field_type == BaseType.Vector:
                builder.PrependUOffsetTRelativeSlot(slot, flatbuffers.number_types.UOffsetTFlags.py_type(encoded_value), 0)
            else:
                raise NotImplementedError('Unsupported field type %s' % field_type)
        object_encoded = builder.EndObject()
        builder.Finish(object_encoded)
        return bytes(builder.Output())

    def _encode_field(self, builder: flatbuffers.Builder, value: Any, field: Field) -> int:
        field_type = field.Type().BaseType()
        if field_type == BaseType.UByte or field_type == BaseType.Int or field_type == BaseType.UInt or field_type == BaseType.ULong:
            return value
        elif field_type == BaseType.String:
            return builder.CreateString(value)
        elif field_type == BaseType.Vector:
            return builder.CreateByteVector(value)
        else:
            raise NotImplementedError(f'Unsupported field type: {field_type}')