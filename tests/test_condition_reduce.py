from boto3.dynamodb.conditions import Attr
from functools import reduce


def test_condition_reduce():
    lista = [Attr('f1').eq(1),
             Attr('f2').eq(2)]

    combinacao = lista[0] | lista[1]
    reduced = reduce(lambda red, cur: red | cur, lista)

    assert combinacao == reduced
