import argparse
import logging
from typing import Annotated, ClassVar, Optional, Type

import pytest
from pydantic import BaseModel, ConfigDict, Field

from src.deployments.experiments.base_experiment import BaseExperiment
from src.deployments.utils.parser import ARG_NOT_SET, _field_to_arg, _unwrap_optional, get_from_str

logger = logging.getLogger(__name__)

# Tests for parsing complex objects in ExpConfigs.


class ImageJSON(BaseModel):
    supported_input_kinds: ClassVar[set[str]] = {"json"}

    repo: str
    tag: str


class ImageFromStr:
    supported_input_kinds: ClassVar[set[str]] = {"from_str"}

    repo: str
    tag: str

    @staticmethod
    def from_str(image: str):
        repo, tag = image.split(":")
        obj = ImageFromStr()
        obj.repo = repo
        obj.tag = tag
        return obj


class ImageDirect:
    supported_input_kinds: ClassVar[set[str]] = {"direct"}

    def __init__(self, image: str):
        repo, tag = image.split("-")
        self.repo = repo
        self.tag = tag


class ImageJSONAndFromStr(BaseModel):
    supported_input_kinds: ClassVar[set[str]] = {"json", "from_str"}

    repo: str
    tag: str

    @staticmethod
    def from_str(image: str):
        repo, tag = image.split(":")
        return ImageJSONAndFromStr(repo=repo, tag=tag)


class ImageNone:
    supported_input_kinds: ClassVar[set[str]] = set()


def get_exp_conf(image_type):
    namespace = {
        "__annotations__": {
            "image": image_type,
            "supported_input_kinds": ClassVar[set[str]],
        },
        "image": Field(description="Container image"),
        "supported_input_kinds": getattr(image_type, "supported_input_kinds", set()),
        "model_config": ConfigDict(use_attribute_docstrings=True, arbitrary_types_allowed=True),
    }
    return type(f"ExpConfig_{image_type.__name__}", (BaseModel,), namespace)


@pytest.fixture(scope="class")
def TestBaseExp(request):
    config_cls = request.param
    DynamicClass = type(f"DynamicTestExp_{config_cls.__name__}", (BaseExperiment[config_cls],), {})
    DynamicClass.config_cls = config_cls
    DynamicClass.name = f"TestBaseExpWith{config_cls.__name__}"
    return DynamicClass


class TestGetFromStr:
    def test_json_basemodel(self):
        converter = get_from_str(ImageJSON, "image")
        result = converter('{"repo": "nWaku", "tag": "v0.36"}')
        assert result.repo == "nWaku"
        assert result.tag == "v0.36"

    def test_from_str_basemodel(self):
        converter = get_from_str(ImageFromStr, "image")
        result = converter("nWaku:v0.36")
        assert result.repo == "nWaku"
        assert result.tag == "v0.36"

    def test_direct_constructor(self):
        converter = get_from_str(ImageDirect, "image")
        result = converter("nWaku-v0.36")
        assert result.repo == "nWaku"
        assert result.tag == "v0.36"

    def test_all_three_uses_json_first(self):
        converter = get_from_str(ImageJSONAndFromStr, "image")
        result = converter('{"repo": "nWaku", "tag": "v0.36"}')
        assert result.repo == "nWaku"
        assert result.tag == "v0.36"

    def test_none_fails(self):
        converter = get_from_str(ImageNone, "image")
        with pytest.raises(argparse.ArgumentTypeError):
            converter("nWaku:v0.36")


class _HelpFormatter(argparse.HelpFormatter):
    def __init__(self, prog):
        super().__init__(prog, max_help_position=100, width=220)


@pytest.mark.parametrize(
    "TestBaseExp",
    [
        get_exp_conf(ImageJSONAndFromStr),
        get_exp_conf(ImageJSON),
        get_exp_conf(ImageFromStr),
        get_exp_conf(ImageDirect),
        get_exp_conf(ImageNone),
    ],
    indirect=True,
)
class TestImageInArgparse:
    def test_help_output_contains_config_params(self, capsys, TestBaseExp: Type[BaseExperiment]):
        parser = argparse.ArgumentParser(
            description="Test description", formatter_class=_HelpFormatter
        )
        subparsers = parser.add_subparsers(dest="experiment", required=True)
        subparser = subparsers.add_parser(TestBaseExp.__name__, help=TestBaseExp.__doc__)
        TestBaseExp.add_config_args(subparser)

        with pytest.raises(SystemExit):
            parser.parse_args([TestBaseExp.__name__, "--help"])
        captured = capsys.readouterr()

        assert "--image" in captured.out
        assert "Container image" in captured.out

    @pytest.mark.parametrize(
        "input_kind, input_value",
        [
            ("json", '{"repo": "nWaku", "tag": "v0.36"}'),
            ("from_str", "nWaku:v0.36"),
            ("direct", "nWaku-v0.36"),
        ],
    )
    def test_cli_parsing(self, input_kind, input_value, TestBaseExp: Type[BaseExperiment]):
        parser = argparse.ArgumentParser(description="Test parser", formatter_class=_HelpFormatter)
        subparsers = parser.add_subparsers(dest="experiment", required=True)
        subparser = subparsers.add_parser(TestBaseExp.name, help=TestBaseExp.__doc__)
        TestBaseExp.add_config_args(subparser)

        command = [TestBaseExp.name, "--image", input_value]
        should_pass = input_kind in TestBaseExp.config_cls.supported_input_kinds
        if should_pass:
            args = parser.parse_args(command)
            assert args.image.repo == "nWaku"
            assert args.image.tag == "v0.36"
        else:
            with pytest.raises(SystemExit):
                parser.parse_args(command)


class TestParserTypeWiring:
    def test_complex_type_gets_get_from_str_callable(self):
        field_info = type(
            "MockFieldInfo",
            (),
            {
                "annotation": ImageDirect,
                "description": "Container image",
                "default": ARG_NOT_SET,
            },
        )()

        flag, kwargs = _field_to_arg("image", field_info)
        assert flag == "--image"
        assert callable(kwargs["type"])
        assert kwargs["metavar"] == "(ImageDirect)"

    def test_bool_does_not_get_get_from_str(self):
        field_info = type(
            "MockFieldInfo",
            (),
            {
                "annotation": bool,
                "description": "Flag",
                "default": False,
            },
        )()

        flag, kwargs = _field_to_arg("flag", field_info)
        assert flag == "--flag"
        assert kwargs["action"] == "store_true"
        assert "type" not in kwargs

    def test_primitive_types_use_native_types(self):
        for py_type in (int, float, str):
            field_info = type(
                "MockFieldInfo",
                (),
                {
                    "annotation": py_type,
                    "description": f"{py_type.__name__} field",
                    "default": ARG_NOT_SET,
                },
            )()

            _, kwargs = _field_to_arg("field", field_info)
            assert kwargs["type"] == py_type

    # Container types - should parse JSON
    def test_list_str_parses_json(self):
        """list[str] should parse JSON strings."""
        field_info = type(
            "MockFieldInfo",
            (),
            {
                "annotation": list[str],
                "description": "List field",
                "default": ARG_NOT_SET,
            },
        )()

        flag, kwargs = _field_to_arg("items", field_info)
        assert flag == "--items"
        result = kwargs["type"]('["a", "b"]')
        assert result == ["a", "b"]

    def test_dict_str_str_parses_json(self):
        """dict[str, str] should parse JSON strings."""
        field_info = type(
            "MockFieldInfo",
            (),
            {
                "annotation": dict[str, str],
                "description": "dict field",
                "default": ARG_NOT_SET,
            },
        )()

        flag, kwargs = _field_to_arg("metadata", field_info)
        assert flag == "--metadata"
        result = kwargs["type"]('{"key": "value"}')
        assert result == {"key": "value"}

    def test_set_str_parses_json(self):
        """set[str] should parse JSON strings (returns list from JSON)."""
        field_info = type(
            "MockFieldInfo",
            (),
            {
                "annotation": set[str],
                "description": "set field",
                "default": ARG_NOT_SET,
            },
        )()

        flag, kwargs = _field_to_arg("tags", field_info)
        assert flag == "--tags"
        result = kwargs["type"]('["a", "b"]')
        assert result == ["a", "b"]  # JSON gives list, Pydantic converts to set later

    def test_bare_list_parses_json(self):
        """Bare list should parse JSON strings."""
        field_info = type(
            "MockFieldInfo",
            (),
            {
                "annotation": list,
                "description": "Bare list",
                "default": ARG_NOT_SET,
            },
        )()

        flag, kwargs = _field_to_arg("items", field_info)
        assert flag == "--items"
        result = kwargs["type"]('["a", 1, true]')
        assert result == ["a", 1, True]

    # Typing constructs - should not crash
    def test_optional_int_parses(self):
        """Optional[int] should unwrap and parse as int."""
        field_info = type(
            "MockFieldInfo",
            (),
            {
                "annotation": Optional[int],
                "description": "Optional int",
                "default": ARG_NOT_SET,
            },
        )()

        flag, kwargs = _field_to_arg("value", field_info)
        assert flag == "--value"
        result = kwargs["type"]("42")
        assert result == 42

    def test_annotated_str_uses_str(self):
        """Annotated[str, ...] should unwrap and use str."""
        field_info = type(
            "MockFieldInfo",
            (),
            {
                "annotation": Annotated[str, "metadata"],
                "description": "Annotated string",
                "default": ARG_NOT_SET,
            },
        )()

        flag, kwargs = _field_to_arg("data", field_info)
        assert flag == "--data"
        assert kwargs["type"] == str

    def test_annotated_list_parses_json(self):
        """Annotated[list[str], ...] should unwrap and parse JSON."""
        field_info = type(
            "MockFieldInfo",
            (),
            {
                "annotation": Annotated[list[str], "metadata"],
                "description": "Annotated list",
                "default": ARG_NOT_SET,
            },
        )()

        flag, kwargs = _field_to_arg("items", field_info)
        assert flag == "--items"
        result = kwargs["type"]('["a", "b"]')
        assert result == ["a", "b"]

    def test_from_str_falls_back_on_failure(self):
        """If from_str raises an error, should fall back to constructor."""

        class CustomClass:
            @staticmethod
            def from_str(s: str):
                raise ValueError("intentional failure")

            def __init__(self, s: str):
                self.value = s

        converter = get_from_str(CustomClass, "data")
        result = converter("test")
        assert result.value == "test"  # Should use constructor fallback


class TestUnwrapOptional:
    """Test _unwrap_optional handles various type annotations."""

    def test_unwraps_deeply_nested(self):
        """Annotated[Optional[Annotated[T, ...]], ...] should unwrap to T."""
        result = _unwrap_optional(Annotated[Optional[Annotated[str, "inner"]], "outer"])
        assert result is str

    def test_unwraps_custom_class_optional(self):
        """CustomClass | None should unwrap to CustomClass."""

        class ImageConfig:
            pass

        result = _unwrap_optional(ImageConfig | None)
        assert result is ImageConfig

    def test_non_wrapped_unchanged(self):
        """Plain types should be unchanged."""
        assert _unwrap_optional(str) is str
        assert _unwrap_optional(int) is int
        assert _unwrap_optional(list[str]) == list[str]
