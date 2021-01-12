from clean_architecture_basic_classes import BasicEntity
from clean_architecture_dynamodb_adapter import BasicDynamodbAdapter
from collections import namedtuple
from marshmallow import fields, post_load
from pytest import fixture
from typing import Optional
from unittest.mock import MagicMock


AdapterFactory = namedtuple('Factory', 'adapter, '
                                       'entity_cls, '
                                       'table_name, '
                                       'db_endpoint')


@fixture(scope='class')
def adapter_factory(request):
    def factory(table_name=MagicMock(), db_endpoint=None):
        class DummyEntity(BasicEntity):
            def __init__(self,
                         float_value: float,
                         entity_id: Optional[str] = None):
                super().__init__(entity_id=entity_id)
                self.float_value = float_value

            class Schema(BasicEntity.Schema):
                float_value = fields.Float(required=True)

                @post_load
                def on_load(self, data, many, partial):
                    return DummyEntity(**data)

        class Adapter(BasicDynamodbAdapter):
            def __init__(self, table_name, db_endpoint):
                super().__init__(table_name=table_name,
                                 db_endpoint=db_endpoint,
                                 adapted_class=DummyEntity)

        adapter = Adapter(table_name=table_name, db_endpoint=db_endpoint)
        return AdapterFactory(adapter=adapter,
                              entity_cls=DummyEntity,
                              table_name=table_name,
                              db_endpoint=db_endpoint)

    request.cls.factory = factory


@fixture(scope='class')
def dynamo_entity(request):
    def factory(value):
        json_entity = {
            'entity_id': 'meu_id',
            'float_value': f'Float({value})'
        }
        return json_entity

    request.cls.dynamo_entity_factory = staticmethod(factory)
