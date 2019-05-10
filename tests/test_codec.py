import faust

from faust_codec_flatbuffers.codec import FlatbuffersCodec


class Data(faust.Record):
    id: str
    number: int


def test_dumps():
    model = Data(id='abcd', number=1234)
    codec = FlatbuffersCodec(model)
    data = model.to_representation()

    binary = codec.dumps(data)

    expected = b'\x0c\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x04\x00\x08\x00\x00\x00\xd2\x04\x00\x00\x04\x00\x00\x00\x04\x00\x00\x00abcd\x00\x00\x00\x00'
    assert binary == expected


    binary = codec.dumps(model.to_representation())

    assert model.id.encode() in binary
