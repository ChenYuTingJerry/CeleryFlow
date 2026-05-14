"""Tests for string / dict case conversion helpers."""
from __future__ import annotations

from celeryflow.utils import DictConvert, StringConvert


class TestStringConvert:
    def test_to_snake_case(self):
        assert StringConvert.to_snake_case("CamelCase") == "camel_case"
        assert StringConvert.to_snake_case("lowerCamelCase") == "lower_camel_case"
        assert StringConvert.to_snake_case("HTTPServer") == "http_server"
        assert StringConvert.to_snake_case("simpleword") == "simpleword"

    def test_to_lower_camel_case(self):
        assert StringConvert.to_lower_camel_case("snake_case_name") == "snakeCaseName"
        assert StringConvert.to_lower_camel_case("simple") == "simple"

    def test_to_upper_camel_case(self):
        assert StringConvert.to_upper_camel_case("snake_case_name") == "SnakeCaseName"
        assert StringConvert.to_upper_camel_case("simple") == "Simple"


class TestDictConvert:
    def test_snake_nested(self):
        src = {"firstName": "A", "addr": {"streetName": "x", "zipCode": "1"}}
        out = DictConvert.to_snake_case(src)
        assert out == {"first_name": "A", "addr": {"street_name": "x", "zip_code": "1"}}

    def test_list_recursion(self):
        src = {"items": [{"itemId": 1}, {"itemId": 2}]}
        out = DictConvert.to_snake_case(src)
        assert out == {"items": [{"item_id": 1}, {"item_id": 2}]}

    def test_upper_camel(self):
        src = {"first_name": "A", "items": [{"item_id": 1}]}
        out = DictConvert.to_upper_camel_case(src)
        assert out == {"FirstName": "A", "Items": [{"ItemId": 1}]}

    def test_non_dict_passthrough(self):
        assert DictConvert.to_snake_case("plain string") == "plain string"
        assert DictConvert.to_snake_case(123) == 123
