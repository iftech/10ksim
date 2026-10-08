# Python Imports
import argparse
import json
import types
from typing import Annotated, Any, Iterable, List, Literal, Union, get_args, get_origin

from pydantic import BaseModel, ValidationError
from pydantic.fields import FieldInfo

from src.utils.yaml_utils import get_YAML

ARG_NOT_SET = object()


def _annotation_display(annotation) -> str:
    annotation = _unwrap_optional(annotation)
    origin = get_origin(annotation)

    if annotation in (int, float, str, bool):
        return f"({annotation.__name__})"

    if origin is Literal:
        values = ", ".join(repr(v) for v in get_args(annotation))
        return f"(choices: [{values}])"

    if hasattr(annotation, "__name__"):
        return f"({annotation.__name__})"

    return str(annotation)


def _unwrap_optional(annotation):
    """Recursively unwrap Annotated and Optional to get the actual type."""
    origin = get_origin(annotation)

    # Unwrap Annotated[T, ...] -> T, then recurse
    if origin is Annotated:
        args = get_args(annotation)
        if args:
            return _unwrap_optional(args[0])

    # Unwrap Optional[T] -> T
    # Optional[T] => typing.Union
    # T | None => types.UnionType
    if origin is Union or origin is types.UnionType:
        args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(args) == 1:
            return _unwrap_optional(args[0])

    return annotation


def get_from_str(annotation: Any, field_name: str):
    def from_str(input_str: str) -> Any:
        errors = []

        # Convert using model_validate_json.
        try:
            if isinstance(annotation, type) and issubclass(annotation, BaseModel):
                return annotation.model_validate_json(input_str)
        except (ValidationError, ValueError) as e:
            errors.append(
                argparse.ArgumentTypeError(
                    f"model_validate_json failed. Field: `{field_name}` Error: `{e}`"
                )
            )

        # Convert using custom from_str method.
        from_str_method = getattr(annotation, "from_str", None)
        if from_str_method is not None:
            try:
                return from_str_method(input_str)
            except Exception as e:
                errors.append(
                    argparse.ArgumentTypeError(
                        f"from_str failed. Field: `{field_name}` Error: `{e}`"
                    )
                )

        # Convert using direct instantiation.
        if callable(annotation):
            try:
                return annotation(input_str)
            except Exception as e:
                err = argparse.ArgumentTypeError(
                    f"Failed to convert string directly. Field: `{field_name}` Error: `{e}`"
                )
                errors.append(err)

        if len(errors) == 1:
            raise errors[0]
        raise argparse.ArgumentTypeError(f"Failed to convert field from str. Errors: {errors}")

    return from_str


def _field_to_arg(field_name: str, field: FieldInfo) -> tuple[str, dict[str, Any]]:
    annotation = _unwrap_optional(field.annotation)
    flag = f"--{field_name.replace('_', '-')}"

    kwargs: dict[str, Any] = {
        "dest": field_name,
        "default": ARG_NOT_SET,
        "required": False,
    }

    if annotation is bool:
        kwargs["action"] = "store_true"
        # For bool, "type" should not be set.

    if annotation in (int, float, str):
        kwargs["type"] = annotation

    origin = get_origin(annotation)
    if origin is Union:
        args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(args) == 1 and args[0] in (int, float, str):
            kwargs["type"] = args[0]

    if origin is Literal:
        choices = get_args(annotation)
        if choices:
            kwargs["type"] = type(choices[0])
            kwargs["choices"] = choices

    # Container types - parse as JSON
    if origin in (list, set, dict, tuple) or annotation in (list, set, dict, tuple):
        kwargs["type"] = lambda item: json.loads(item)

    # Fallback for complex types (only if annotation is a class)
    elif "type" not in kwargs and annotation is not bool:
        if isinstance(annotation, type):
            kwargs["type"] = get_from_str(annotation, field_name)
        # Otherwise, keep as string (typing constructs like Annotated, etc.)
        else:
            kwargs["type"] = str

    if field.description:
        kwargs["help"] = field.description

    type_label = _annotation_display(annotation)
    if "type" in kwargs:
        kwargs["metavar"] = f"{type_label}"

    return flag, kwargs


def _config_model_fields_to_args(config_model: type[BaseModel]) -> list[tuple[str, dict[str, Any]]]:
    args = []
    for field_name, field in config_model.model_fields.items():
        args.append(_field_to_arg(field_name, field))
    return args


def with_experiment_from_values(
    argv: List[str], top_level: argparse.ArgumentParser, names: Iterable[str]
) -> List[str]:
    """Add the values file's `experiment` as the subcommand when the CLI names none."""
    known, rest = top_level.parse_known_args(argv)
    if (rest and rest[0] in names) or known.values_path is None:
        return argv
    with open(known.values_path) as values:
        name = (get_YAML().load(values) or {}).get("experiment")
    if name is None:
        return argv

    # Top-level options must come before the subcommand, the experiment's own after it.
    top, matched = [], 0
    for token in argv:
        if matched < len(rest) and token == rest[matched]:
            matched += 1
        else:
            top.append(token)
    return top + [name] + rest
