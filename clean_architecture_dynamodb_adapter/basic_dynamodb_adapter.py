from functools import reduce
from uuid import uuid4

import boto3
from boto3.dynamodb.conditions import Attr
# noinspection PyPackageRequirements
from botocore.exceptions import ClientError
from clean_architecture_basic_classes.basic_persist_adapter import BasicPersistAdapter


class BasicDynamodbAdapter(BasicPersistAdapter):
    def __init__(self, table_name, db_endpoint, adapted_class, logger=None):
        """
        Adapter para persistencia de um entity
        :param table_name: Nome da tabela à ser usada
        """
        super().__init__(adapted_class, logger)
        self._table_name = table_name
        self._db_endpoint = db_endpoint
        self._db = self.get_db()
        self._table = self.get_table()

        self._create_table_if_dont_exists()

    def _do_table_exists(self):
        existing_tables = boto3.client(
            'dynamodb', endpoint_url=self._db_endpoint).list_tables()
        return self._table_name in existing_tables['TableNames']

    def _create_table_if_dont_exists(self):
        if not self._do_table_exists():
            self.logger.info(f'Creating not existent table {self._table_name}')

            table = self._db.create_table(
                TableName=self._table_name,
                KeySchema=[
                    {
                        'AttributeName': 'entity_id',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'entity_id',
                        'AttributeType': 'S'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )

            # Wait until the table exists.
            table.meta.client.get_waiter('table_exists').wait(
                TableName=self._table_name)

    def get_db(self):
        return boto3.resource('dynamodb', endpoint_url=self._db_endpoint)

    def get_table(self):
        return self._db.Table(self._table_name)

    def _instantiate_object(self, x):
        denormed = BasicDynamodbAdapter._denormalize_floats(x)
        obj = self._class.from_json(denormed)
        obj.set_adapter(self)
        return obj

    def list_all(self):
        response = self._table.scan()
        objects = [self._instantiate_object(x) for x in response['Items']]
        return objects

    def get_by_id(self, item_id):
        response = self._table.get_item(Key=dict(entity_id=item_id),
                                        ConsistentRead=True)
        if 'Item' in response:
            return self._instantiate_object(response['Item'])
        else:
            return None

    @staticmethod
    def _denormalize_floats_on_set(arg):
        return {BasicDynamodbAdapter._denormalize_floats(x) for x in arg}

    @staticmethod
    def _denormalize_floats_on_list(arg):
        return [BasicDynamodbAdapter._denormalize_floats(x) for x in arg]

    @staticmethod
    def _denormalize_floats_on_dict(arg):
        return {k: BasicDynamodbAdapter._denormalize_floats(v)
                for k, v in arg.items()}

    @staticmethod
    def _denormalize_float(arg):
        if not isinstance(arg, str):
            return arg
        if arg.startswith('Float(') and arg.endswith(')'):
            return float(arg[6:-1])
        else:
            return arg

    @staticmethod
    def _denormalize_floats(arg):
        cleaners = {set: BasicDynamodbAdapter._denormalize_floats_on_set,
                    list: BasicDynamodbAdapter._denormalize_floats_on_list,
                    dict: BasicDynamodbAdapter._denormalize_floats_on_dict,
                    float: BasicDynamodbAdapter._denormalize_float}

        arg_type = type(arg)
        if arg_type in cleaners:
            return cleaners[arg_type](arg)
        else:
            return BasicDynamodbAdapter._denormalize_float(arg)

    @staticmethod
    def _clean_set_empty_elements(arg):
        arg = set(x for x in arg if not hasattr(x, '__len__') or
                  len(x) > 0)
        return arg

    @staticmethod
    def _clean_list_empty_elements(arg):
        result = []
        for value in arg:
            clean_value = BasicDynamodbAdapter._normalize_nodes(value)
            if clean_value:
                result.append(clean_value)
        return result

    @staticmethod
    def _clean_dict_empty_elements(arg):
        result = {}
        for key, value in arg.items():
            clean_value = BasicDynamodbAdapter._normalize_nodes(value)
            if clean_value:
                result.update({key: clean_value})
        return result

    @staticmethod
    def _clean_float_value(arg):
        return f'Float({arg})'

    @staticmethod
    def _normalize_nodes(arg):
        cleaners = {set: BasicDynamodbAdapter._clean_set_empty_elements,
                    list: BasicDynamodbAdapter._clean_list_empty_elements,
                    dict: BasicDynamodbAdapter._clean_dict_empty_elements,
                    float: BasicDynamodbAdapter._clean_float_value}

        arg_type = type(arg)
        if arg_type in cleaners:
            return cleaners[arg_type](arg)

        if not hasattr(arg, '__len__') or len(arg) != 0:
            return arg
        else:
            return None

    def save(self, json_data):
        entity_id = json_data.get('entity_id', str(uuid4()))
        json_data.update(dict(entity_id=entity_id))
        self.logger.debug(f'Data received to save: {json_data}')
        cleaned_data = BasicDynamodbAdapter._normalize_nodes(json_data)
        self.logger.debug(f'Saving after remove empties: {json_data}')
        self._table.put_item(Item=cleaned_data)
        return entity_id

    def delete(self, entity_id):
        try:
            self._table.delete_item(Key=dict(entity_id=entity_id))
        except ClientError as e:
            error = e.response['Error']['Message']
            self._logger.error(f'Erro deletando de {self._class.__name__}: '
                               f'{error}')
            return None
        return entity_id

    @staticmethod
    def _get_ops():
        return {'begins_with': 1,
                'between': 2,
                'contains': 1,
                'eq': 1,
                'exists': 0,
                'gt': 1,
                'gte': 1,
                'is_in': 1,
                'lt': 1,
                'lte': 1,
                'ne': 1,
                'not_exists': 0,
                'size': 0}

    @staticmethod
    def _args_from_value(value, arg_count):
        args = []
        if arg_count == 1:
            args.append(value)
        elif arg_count > 1:
            args.extend(value)

        return args

    @staticmethod
    def _get_scan_kwargs(filter_cond, kwargs):
        scan_kwargs = {
            'FilterExpression': filter_cond
        }
        if 'ProjectionExpression' in kwargs:
            scan_kwargs.update({
                'ProjectionExpression': kwargs['ProjectionExpression']
            })
        else:
            scan_kwargs.update({
                'Select': 'ALL_ATTRIBUTES'
            })
        return scan_kwargs

    @staticmethod
    def _get_argcount(op, ops):
        try:
            return ops[op]
        except KeyError:
            raise ValueError(f'Comparador inválido: {op}')

    @staticmethod
    def _get_contitions(kwargs):
        ops = BasicDynamodbAdapter._get_ops()
        have_projection = False
        conditions = []

        for k, v in kwargs.items():
            if k == 'ProjectionExpression':
                have_projection = True
                continue

            field, op = k.split('__')
            arg_count = BasicDynamodbAdapter._get_argcount(op, ops)

            args = BasicDynamodbAdapter._args_from_value(v, arg_count)
            field = field.replace('_dot_', '.')
            conditions.append(getattr(Attr(field), op)(*args))

        if not conditions:
            raise ValueError('Nenhuma condição no filtro.')

        return (have_projection,
                reduce(lambda accum, curr: accum | curr, conditions),)

    def _desserialize(self, result):
        objects = [self._class.from_json(x) for x in result]
        for obj in objects:
            obj.set_adapter(self)

        return objects

    def filter(self, **kwargs):
        """
        Filtra objetos de acordo com o critério especificado.
        Para especificar o critérios, que por default são concatenados
        com o operador lógico *ou*, use o nome do campo junto com o operador
        desejado concatenado com um "__" (duplo sublinha).
        Exemplo: Para filtrar todos os objetos em que o campo email seja
        igual à "nome@dom.com", o filtro deverá ser chamado assim:
            result = adapter.filter(email__eq="nome@dom.com")

        :raises ValueError(Comparador inválido): se o comparador especificado
            não for um dos seguintes:
               [begins_with, between, contains, eq, exists, gt, gte, is_in, lt,
                lte, ne, not_exists]

        :return: Lista de objetos
        """
        have_projection, conditions = self._get_contitions(kwargs)
        scan_kwargs = self._get_scan_kwargs(conditions, kwargs)
        result = self._table.scan(**scan_kwargs)['Items']

        if have_projection:
            return result

        return self._desserialize(result)

    class DynamodbAdapterScanException(BaseException):
        pass
