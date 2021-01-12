from tests.conftest import AdapterFactory
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.usefixtures('adapter_factory', 'dynamo_entity')
class TestFloatValue(TestCase):
    def setUp(self):
        self.patch_boto3 = patch('clean_architecture_dynamodb_adapter.'
                                 'basic_dynamodb_adapter.boto3')
        self.mock_boto3 = self.patch_boto3.start()

    def tearDown(self):
        self.patch_boto3.stop()

    @patch('clean_architecture_basic_classes.basic_domain.basic_entity.uuid4')
    def test_saving_float_value(self, mock_uuid4):
        value = 10 / 3.0
        fac: AdapterFactory = self.factory()
        entity = fac.entity_cls(float_value=value)
        entity.set_adapter(fac.adapter)

        entity.save()

        mock_uuid4.assert_called_once()
        mock_table = self.mock_boto3.resource().Table()
        mock_table.put_item.assert_called_with(
            Item={
                'float_value': f'Float({value})',
                'entity_id': str(mock_uuid4())
            }
        )

    def test_get_by_id_float_value(self):
        value = 10 / 3.0
        expected_scan_result = {
            'Item': self.dynamo_entity_factory(value)
        }
        fac: AdapterFactory = self.factory()
        mock_entity_id = MagicMock()

        mock_table = self.mock_boto3.resource().Table()
        mock_table.get_item = MagicMock(return_value=expected_scan_result)

        entity = fac.adapter.get_by_id(mock_entity_id)

        self.assertEqual(entity.float_value, value)

    def test_list_all_float_value(self):
        value1 = 10 / 3.0
        value2 = 8 / 3.0
        expected_scan_result = {
            'Items': [self.dynamo_entity_factory(value1),
                      self.dynamo_entity_factory(value2)]
        }
        fac: AdapterFactory = self.factory()

        mock_table = self.mock_boto3.resource().Table()
        mock_table.scan = MagicMock(return_value=expected_scan_result)

        entities = fac.adapter.list_all()

        self.assertEqual(entities[0].float_value, value1)
        self.assertEqual(entities[1].float_value, value2)
