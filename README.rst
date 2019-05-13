|build-status| |coverage|

This package is a plugin for the stream processing framework `Faust`_. It provides a codec to serialize and deserialize Faust models using `Flatbuffers`_.

.. warning:: This package is highly experimental. Do not use it in production systems.

.. _`Faust`: https://faust.readthedocs.io/
.. _`Flatbuffers`: https://google.github.io/flatbuffers/

Usage
=====
.. code-block:: Python

  import faust
  from faust_codec_flatbuffers import FlatbuffersCodec

  class Point(faust.Record):
      x: int
      y: int

  faust.serializers.codecs.register('point', FlatbuffersCodec(Point))


.. |build-status| image:: https://secure.travis-ci.org/digitalernachschub/faust-codec-flatbuffers.png
    :alt: Build status
    :target: https://travis-ci.org/digitalernachschub/faust-codec-flatbuffers

.. |coverage| image:: https://codecov.io/gh/digitalernachschub/faust-codec-flatbuffers/branch/master/graphs/badge.svg
    :alt: Test coverage
    :target: https://codecov.io/gh/digitalernachschub/faust-codec-flatbuffers
