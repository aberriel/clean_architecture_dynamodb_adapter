from botocore.exceptions import ClientError
from clean_architecture_dynamodb_adapter import BasicDynamodbAdapter
from math import pi
from pytest import raises
from tests.conftest import AdapterFactory
from unittest import TestCase
from unittest.mock import patch, MagicMock

# noinspection PyPackageRequirements
import pytest


@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
def test_basic_dynamicdb_adapter(mock_boto3):
    logger = MagicMock()
    adapter = BasicDynamodbAdapter('tabela', None, MagicMock(), logger)

    mock_boto3.client.assert_called_once()
    mock_boto3.client.return_value.list_tables.assert_called_once()
    mock_boto3.resource.assert_called_once()
    mock_boto3.resource.assert_called_with('dynamodb', endpoint_url=None)

    assert adapter._table_name == 'tabela'


# noinspection PyUnusedLocal
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
def test_list_all(mock_boto3):
    logger = MagicMock()
    dummy_class = MagicMock()
    adapter = BasicDynamodbAdapter('tabela', None, dummy_class, logger)

    scan_result = {'Items': [1, 2, 3, 4]}
    with patch.object(adapter, '_table') as mock:
        mock.scan = MagicMock(return_value=scan_result)
        result = adapter.list_all()

    mock.scan.assert_called_once()
    dummy_class.from_json.assert_called()
    result[0].set_adapter.assert_called_with(adapter)


# noinspection PyUnusedLocal
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
@patch.object(BasicDynamodbAdapter, '_instantiate_object')
def test_get_by_id(mock_instantiate_object, mock_boto3):
    dummy_class = MagicMock()
    dummy_class.from_json = lambda x: x
    adapter = BasicDynamodbAdapter('tabela', None, dummy_class, MagicMock())

    scan_result = {'Item': MagicMock(entity_id=42)}
    with patch.object(adapter, '_table') as mock:
        mock.get_item = MagicMock(return_value=scan_result)
        result = adapter.get_by_id(42)

    mock.get_item.assert_called_with(Key=dict(entity_id=42),
                                     ConsistentRead=True)
    assert result == mock_instantiate_object()


@pytest.mark.usefixtures('adapter_factory', 'dynamo_entity')
class TestGetByIdSetAdapter(TestCase):
    def setUp(self):
        self.patch_boto3 = patch('clean_architecture_dynamodb_adapter.'
                                 'basic_dynamodb_adapter.boto3')
        self.mock_boto3 = self.patch_boto3.start()

    def tearDown(self):
        self.patch_boto3.stop()

    def test_get_by_id_set_adapter(self):
        expected_scan_result = {
            'Item': self.dynamo_entity_factory(1)
        }
        fac: AdapterFactory = self.factory()
        mock_entity_id = MagicMock()

        mock_table = self.mock_boto3.resource().Table()
        mock_table.get_item = MagicMock(return_value=expected_scan_result)

        entity = fac.adapter.get_by_id(mock_entity_id)

        self.assertIsInstance(entity, fac.entity_cls)
        self.assertEqual(entity.adapter, fac.adapter)

    def test_list_all_set_adapter(self):
        expected_scan_result = {
            'Items': [self.dynamo_entity_factory(1),
                      self.dynamo_entity_factory(1)]
        }
        fac: AdapterFactory = self.factory()

        mock_table = self.mock_boto3.resource().Table()
        mock_table.scan = MagicMock(return_value=expected_scan_result)

        entities = fac.adapter.list_all()

        for entity in entities:
            self.assertEqual(entity.adapter, fac.adapter)


# noinspection PyUnusedLocal
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
def test_get_by_id_not_found(mock_boto3):
    adapter = BasicDynamodbAdapter('tabela', None, MagicMock(), MagicMock())

    scan_result = {'Not Found': True}
    with patch.object(adapter, '_table') as mock:
        mock.get_item = MagicMock(return_value=scan_result)
        result = adapter.get_by_id(42)

    assert result is None


# noinspection PyUnusedLocal
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
def test_save(mock_boto3):
    adapter = BasicDynamodbAdapter('tabela', None, MagicMock(), MagicMock())

    with patch.object(adapter, '_table') as mock:
        mock.put_item = MagicMock()
        json_data = {}
        adapter.save(json_data)

    mock.put_item.assert_called_once()


# noinspection PyUnusedLocal
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
def test_delete(mock_boto3):
    adapter = BasicDynamodbAdapter('tabela', None, MagicMock(), MagicMock())

    with patch.object(adapter, '_table') as mock:
        mock.delete_item = MagicMock()
        result = adapter.delete('meu id')

    assert result == 'meu id'
    mock.delete_item.assert_called_with(Key=dict(entity_id='meu id'))


# noinspection PyUnusedLocal
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
def test_delete_raising(mock_boto3):
    logger = MagicMock()

    class DummyClass:
        pass

    adapter = BasicDynamodbAdapter('tabela', None, DummyClass, logger)

    with patch.object(adapter, '_table') as mock:
        mock.delete_item = MagicMock(side_effect=ClientError(
            error_response=dict(Error=dict(Code=500, Message='oops')),
            operation_name='delete'))
        result = adapter.delete('meu id')

    assert result is None
    mock.delete_item.assert_called_with(Key=dict(entity_id='meu id'))
    logger.error.assert_called_once()


# noinspection PyProtectedMember
def test_normalize_nodes_set():
    arg = {1, 2, 3, ''}
    result = BasicDynamodbAdapter._normalize_nodes(arg)
    assert result == {1, 2, 3}


# noinspection PyProtectedMember
def test_normalize_nodes_list():
    arg = [1, 2, 3, '', dict(), []]
    result = BasicDynamodbAdapter._normalize_nodes(arg)
    assert result == [1, 2, 3]


# noinspection PyProtectedMember
def test_normalize_nodes_complexo():
    arg = dict(k1='fica', k2=dict(sk1='fica2', sk2=['', ''], sk3={1, 2, ''}))
    result = BasicDynamodbAdapter._normalize_nodes(arg)

    assert result == dict(k1='fica', k2=dict(sk1='fica2', sk3={1, 2}))


# noinspection PyProtectedMember
def test_normalize_nodes_float():
    valor1 = 10 / 3.0
    valor2 = 8 / 3.0
    valor3 = pi
    arg = {'valor1': valor1, 'valor2': valor2, 'valor3': valor3}
    result = BasicDynamodbAdapter._normalize_nodes(arg)

    assert result == {'valor1': f'Float({valor1})',
                      'valor2': f'Float({valor2})',
                      'valor3': f'Float({valor3})'}


def raise_if_empty(arg):
    if isinstance(arg, (list, set)):
        for value in arg:
            raise_if_empty(arg=value)
    elif isinstance(arg, dict):
        for value in arg.values():
            raise_if_empty(arg=value)

    if hasattr(arg, '__len__') and len(arg) == 0:
        raise ValueError('Item vazio encontrado')


def test_raise_if_empty_raises():
    arg = [1, 2, '']
    with raises(ValueError) as excinfo:
        raise_if_empty(arg)

    assert 'Item vazio encontrado' in str(excinfo.value)


def test_raise_if_empty_raises_with_dict():
    arg = [1, 2, dict(a=1, b='')]
    with raises(ValueError) as excinfo:
        raise_if_empty(arg)

    assert 'Item vazio encontrado' in str(excinfo.value)


def test_denormalize_floats():
    test_json = {
        'dict': {
            'list': [
                'Float(3.5)',
                ['Float(3.5)', 'Float(3.55555)'],
                {'Float(3.5)', 'Float(5.3)', 'Float(2)'},
                'normal',
                42
            ],
            'dict': {
                'Valor': 'Float(3.5)',
                'Normal': 'nada',
                'Numero': 42
            },
            'set': {
                'Float(3.5)',
                'Float(3.6)',
                'normal',
                42
            }
        }
    }

    result = BasicDynamodbAdapter._denormalize_floats(test_json)

    assert result == {
        'dict': {
            'list': [
                3.5,
                [3.5, 3.55555],
                {3.5, 5.3, 2},
                'normal',
                42
            ],
            'dict': {
                'Valor': 3.5,
                'Normal': 'nada',
                'Numero': 42
            },
            'set': {
                3.5,
                3.6,
                'normal',
                42
            }
        }
    }


# noinspection PyUnusedLocal
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
def test_filter(mock_boto):
    adapter = BasicDynamodbAdapter('tabela', None, MagicMock(), MagicMock())
    with patch.object(adapter, '_table') as mock:
        adapter.filter(campo__eq=42, campo2__gt=42)

    mock.scan.assert_called_once()


# noinspection PyUnusedLocal
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
def test_filter_between(mock_boto):
    adapter = BasicDynamodbAdapter('tabela', None, MagicMock(), MagicMock())
    with patch.object(adapter, '_table') as mock:
        adapter.filter(campo__between=[40, 50])

    mock.scan.assert_called_once()


# noinspection PyUnusedLocal
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
def test_filter_exists(mock_boto):
    adapter = BasicDynamodbAdapter('tabela', None, MagicMock(), MagicMock())
    with patch.object(adapter, '_table') as mock:
        adapter.filter(campo__exists=None)

    mock.scan.assert_called_once()


# noinspection PyUnusedLocal
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
def test_filter_projection(mock_boto):
    adapter = BasicDynamodbAdapter('tabela', None, MagicMock(), MagicMock())

    with patch.object(adapter, '_table') as mock:
        adapter.filter(campo__exists=None, ProjectionExpression='campo')

    mock.scan.assert_called_once()


# noinspection PyUnusedLocal
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
def test_filter_invalid_op(mock_boto):
    adapter = BasicDynamodbAdapter('tabela', None, MagicMock(), MagicMock())

    with patch.object(adapter, '_table'):
        with raises(ValueError) as excinfo:
            adapter.filter(campo__oops=42)

    assert 'Comparador inválido: oops' == str(excinfo.value)


# noinspection PyUnusedLocal
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
def test_filter_no_conditions(mock_boto):
    adapter = BasicDynamodbAdapter('tabela', None, MagicMock(), MagicMock())

    with patch.object(adapter, '_table'):
        with raises(ValueError) as excinfo:
            adapter.filter()

    assert 'Nenhuma condição no filtro.' == str(excinfo.value)


# noinspection PyUnusedLocal
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
def test_desserialize(mock_boto3):
    adapter = BasicDynamodbAdapter('tabela', None, MagicMock(), MagicMock())
    mock_class = MagicMock(from_json=MagicMock())
    mock_table = MagicMock(
        scan=MagicMock(
            return_value=dict(Items=[1, 2, 3, 4, 5])))
    with patch.multiple(adapter,
                        _class=mock_class,
                        _table=mock_table):
        result = adapter.filter(field__eq=1)

    for r in result:
        r.set_adapter.assert_called()


@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.Attr')
def test_get_contitions(mock_attr, mock_boto3):
    mock_class = MagicMock()
    mock_logger = MagicMock()
    adapter = BasicDynamodbAdapter('tabela', None, mock_class, mock_logger)

    mock_args = dict(campo__eq=42)
    have_projection, result = adapter._get_contitions(mock_args)

    assert not have_projection

    mock_attr.assert_called_with('campo')
    mock_attr().eq.assert_called_with(42)

    assert result == mock_attr().eq()


@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.boto3')
@patch('clean_architecture_dynamodb_adapter.basic_dynamodb_adapter.Attr')
def test_get_contitions_with_dot(mock_attr, mock_boto3):
    mock_class = MagicMock()
    mock_logger = MagicMock()
    adapter = BasicDynamodbAdapter('tabela', None, mock_class, mock_logger)

    mock_args = dict(campo_dot_subcampo__eq=84)
    have_projection, result = adapter._get_contitions(mock_args)

    assert not have_projection

    mock_attr.assert_called_with('campo.subcampo')
    mock_attr().eq.assert_called_with(84)

    assert result == mock_attr().eq()
