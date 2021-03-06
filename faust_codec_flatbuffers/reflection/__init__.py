from . import Schema
from . import Object
from . import Field
from . import Type


def schema_eq(self, other) -> bool:
    return self.ObjectsLength() == other.ObjectsLength() and \
           all([self.Objects(object_index) == other.Objects(object_index) for object_index in range(self.ObjectsLength())])


def object_eq(self, other) -> bool:
    other_fields = [other.Fields(index) for index in range(other.FieldsLength())]
    return self.Name() == other.Name() and \
        self.FieldsLength() == other.FieldsLength() and \
        all([self.Fields(field_index) in other_fields for field_index in range(self.FieldsLength())])


def field_eq(self, other):
    # TODO: Check attributes for equality
    # TODO: Check documentation for equality
    return self.Name() == other.Name() and \
           self.Type() == other.Type() and \
           self.Offset() == other.Offset() and \
           self.AttributesLength() == other.AttributesLength() and \
           self.DocumentationLength() == other.DocumentationLength()


def type_eq(self, other):
    return self.BaseType() == other.BaseType() and \
           self.Index() == other.Index() and \
           self.Element() == other.Element()


Schema.Schema.__eq__ = schema_eq
Object.Object.__eq__ = object_eq
Field.Field.__eq__ = field_eq
Type.Type.__eq__ = type_eq


def schema_repr(self):
    return 'Schema(objects=[%s])' % ', '.join([repr(self.Objects(object_index)) for object_index in range(self.ObjectsLength())])


def object_repr(self):
    return 'Object(name=%s, fields=[%s])' % (self.Name(), ', '.join([repr(self.Fields(field_index)) for field_index in range(self.FieldsLength())]))


def field_repr(self):
    return 'Field(name=%s, type=%r, offset=%d, attributes=[%s], documentation=[%s])' % \
           (self.Name(), self.Type(), self.Offset(),
            ', '.join([repr(self.Attributes(attr_index)) for attr_index in range(self.AttributesLength())]),
            ', '.join([repr(self.Documentation(index)) for index in range(self.DocumentationLength())])
            )


Schema.Schema.__repr__ = schema_repr
Object.Object.__repr__ = object_repr
Field.Field.__repr__ = field_repr
